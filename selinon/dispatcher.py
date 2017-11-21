#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon Dispatcher task implementation."""

import json
import traceback

from celery import Task

# from .errors import MigrationSkew
# from .errors import MigrationFlowError
# from .errors import MigrationFlowRetry
from .config import Config
from .errors import DispatcherRetry
from .errors import FlowError
from .systemState import SystemState
from .trace import Trace

# from selinonlib.migrations import Migrator


class Dispatcher(Task):
    """Selinon Dispatcher worker implementation."""

    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "selinon.Dispatcher"

    def selinon_retry(self, flow_info, new_retried_count, flow_error):
        """Retry whole flow on failure if configured so, forget any progress done so far.

        :param flow_info: a dictionary holding all the information relevant to flow (dispatcher arguments)
        :type flow_info: dict
        :param new_retried_count: new value for retried count for to flow arguments
        :type new_retried_count: int
        :param flow_error: an error that caused flow to retry (can be None)
        :type flow_error: Exception
        :raises celery.Retry: always
        """
        # pylint: disable=too-many-arguments
        kwargs = {
            'flow_name': flow_info['flow_name'],
            'node_args': flow_info['node_args'],
            'parent': flow_info['parent'],
            'retried_count': new_retried_count,
            'selective': flow_info['selective'],
            'retry': flow_info['retry'],
            'state': flow_info['state']
        }
        countdown = Config.retry_countdown.get(flow_info['flow_name'], 0)
        max_retry = Config.max_retry.get(flow_info['flow_name'], 0)

        if new_retried_count > max_retry:
            # force max_retries to 0 so we are not scheduled and marked as FAILED
            raise self.retry(max_retries=0, exc=flow_error)

        # we will force max retries to None so we are always retried by Celery
        queue = Config.dispatcher_queues[flow_info['flow_name']]
        # TODO: add exception here as well
        Trace.log(Trace.FLOW_RETRY, kwargs, countdown=countdown, queue=queue)
        raise self.retry(kwargs=kwargs,
                         max_retries=None,
                         countdown=countdown,
                         queue=queue)

    def run(self, flow_name, node_args=None, parent=None, retried_count=None, retry=None,
            state=None, selective=False, migration_version=None):
        # pylint: disable=too-many-arguments,arguments-differ
        """Dispatcher entry-point - run each time a dispatcher is scheduled.

        :param flow_name: name of the flow
        :param parent: flow parent nodes
        :param node_args: arguments for workers
        :param retried_count: number of Selinon retries done (not Celery retries)
        :param retry: last retry countdown
        :param state: the current system state
        :param selective: selective flow information if run in selective flow
        :param migration_version: migration version that was used for the flow
        :raises: FlowError
        """
        retried_count = retried_count or 0
        flow_info = {
            'flow_name': flow_name,
            'dispatcher_id': self.request.id,
            'node_args': node_args,
            'retry': retry,
            'queue': Config.dispatcher_queues[flow_name],
            'state': state,
            'selective': selective,
            'retried_count': retried_count,
            'parent': parent,
            'migration_version': migration_version
        }

        Trace.log(Trace.DISPATCHER_WAKEUP, flow_info)

        # Perform migration of state first before proceeding
        # if Config.migration_dir:
        #     # TODO: report something like old_state, new_state here
        #     migrator = Migrator(Config.migration_dir)
        #     state = migrator.perform_migration(flow_name, state, migration_version)
        #     Trace.log(Trace.FLOW_MIGRATION, flow_info)
        # elif migration_version:
        #     # Keep retrying so hopefully some node in the cluster is able to proceed with this message.
        #     Trace.log(Trace.FLOW_MIGRATION_SKEW, flow_info)
        #     raise self.retry(exc=MigrationSkew)

        try:
            system_state = SystemState(self.request.id, flow_name, node_args, retry, state, parent, selective)
            retry = system_state.update()
        except FlowError as exc:
            max_retry = Config.max_retry.get(flow_name, 0)
            Trace.log(Trace.FLOW_FAILURE, flow_info, state=json.loads(str(exc)), will_retry=retried_count < max_retry)
            # Force state to None so dispatcher will run the whole flow again
            flow_info['state'] = None
            raise self.selinon_retry(flow_info=flow_info, new_retried_count=retried_count+1, flow_error=exc)
        except DispatcherRetry as exc:
            if exc.adjust_retry_count:
                retried_count += 1

            if not exc.keep_state:
                flow_info['state'] = None

            raise self.selinon_retry(flow_info, new_retried_count=retried_count, flow_error=exc)
        except Exception as exc:
            Trace.log(Trace.DISPATCHER_FAILURE, flow_info, what=traceback.format_exc())
            raise self.retry(max_retries=0, exc=exc)

        state_dict = system_state.to_dict()
        node_args = system_state.node_args

        if retry is not None and retry >= 0:
            kwargs = {
                'flow_name': flow_name,
                'node_args': node_args,
                'parent': parent,
                'retried_count': retried_count,
                'retry': retry,
                'state': state_dict,
                'selective': system_state.selective
            }
            Trace.log(Trace.DISPATCHER_RETRY, flow_info, kwargs)
            raise self.retry(args=[], kwargs=kwargs, countdown=retry, queue=Config.dispatcher_queues[flow_name])
        else:
            Trace.log(Trace.FLOW_END, flow_info, state=state_dict)
            return {
                'finished_nodes': state_dict['finished_nodes'],
                # this is always {} since we have finished, but leave it here because of failure tracking
                'failed_nodes': state_dict['failed_nodes']
            }
