#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon Dispatcher task implementation."""

import traceback

from .celery import Task
from .config import Config
from .errors import DispatcherRetry
from .errors import FlowError
from .errors import MigrationException
from .errors import MigrationFlowFail
from .errors import MigrationFlowRetry
from .errors import MigrationSkew
from .migrations import Migrator
from .system_state import SystemState
from .trace import Trace


class Dispatcher(Task):
    """Selinon Dispatcher worker implementation."""

    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "selinon.Dispatcher"

    def flow_failure(self, state):
        """Mark the whole flow as failed ignoring retry configuration.

        :param state: flow state that should be captured
        :raises celery.exceptions.Retry: Celery's retry exception, always
        """
        reported_state = {
            'finished_nodes': (state or {}).get('finished_nodes', {}),
            'failed_nodes': (state or {}).get('failed_nodes', {}),
            'active_nodes': (state or {}).get('active_nodes', [])
        }
        exc = FlowError(reported_state)
        raise self.retry(max_retries=0, exc=exc)

    def selinon_retry(self, flow_info, adjust_retried_count=True, keep_state=True):
        """Retry whole flow on failure if configured so, forget any progress done so far.

        :param flow_info: a dictionary holding all the information relevant to flow (dispatcher arguments)
        :type flow_info: dict
        :param adjust_retried_count: if true, retried count will be adjusted, could cause flow failure
        :type adjust_retried_count: bool
        :param keep_state: keep or discard the current progress of the flow
        :type keep_state: bool
        :raises celery.Retry: always
        """
        new_retried_count = flow_info.get('retried_count')
        if adjust_retried_count:
            new_retried_count += 1

        if not keep_state:
            flow_info['state'] = None
            flow_info['retried_count'] = None
            flow_info['retry'] = None

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
            # Force max_retries to 0 so we are not scheduled and marked as FAILED
            raise self.flow_failure(flow_info['state'])

        # We will force max retries to None so we are always retried by Celery
        queue = Config.dispatcher_queues[flow_info['flow_name']]
        # TODO: add exception here as well
        Trace.log(Trace.FLOW_RETRY, kwargs, countdown=countdown, queue=queue)
        raise self.retry(
            kwargs=kwargs,
            max_retries=None,
            countdown=countdown,
            queue=queue
        )

    def migrate_message(self, flow_info):
        """Perform migration of state first before proceeding.

        :param flow_info: information about the current flow
        """
        if Config.migration_dir:
            migrator = Migrator(Config.migration_dir)

            try:
                state, current_migration_version, tainted = migrator.perform_migration(
                    flow_info['flow_name'],
                    flow_info['state'],
                    flow_info['migration_version']
                )
            except MigrationException as exc:
                Trace.log(
                    Trace.MIGRATION_TAINTED_FLOW,
                    flow_info,
                    migration_version=exc.migration_version,
                    latest_migration_version=exc.latest_migration_version,
                    tainting_nodes=exc.tainting_nodes,
                    tainted_edge=exc.tainted_edge,
                    tainted_flow_strategy=exc.TAINTED_FLOW_STRATEGY
                )

                if isinstance(exc, MigrationFlowRetry):
                    raise self.selinon_retry(flow_info, adjust_retried_count=False, keep_state=False)

                if isinstance(exc, MigrationFlowFail):
                    raise self.flow_failure(flow_info['state'])

                raise self.flow_failure(flow_info['state'])
            except MigrationSkew as exc:
                Trace.log(Trace.MIGRATION_SKEW, flow_info, available_migration_version=exc.available_migration_version)
                raise self.selinon_retry(flow_info, adjust_retried_count=False)
            except Exception:
                # If there is anything wrong with migrations, give it a try to be fixed, retry.
                Trace.log(Trace.MIGRATION_ERROR, flow_info, what=traceback.format_exc())
                raise self.selinon_retry(flow_info, adjust_retried_count=False, keep_state=True)

            # Report success of migration
            if current_migration_version != flow_info['migration_version']:
                Trace.log(
                    Trace.MIGRATION,
                    flow_info,
                    new_migration_version=current_migration_version,
                    old_migration_version=flow_info['migration_version'],
                    tainted=tainted
                )
                # Update flow info so we are up2date with migration performed
                flow_info['migration_version'] = current_migration_version
                flow_info['state'] = state

        elif flow_info['migration_version']:
            # Keep retrying so hopefully some node in the cluster is able to proceed with this message.
            Trace.log(Trace.MIGRATION_SKEW, flow_info, available_migration_version=None)
            raise self.selinon_retry(flow_info, adjust_retried_count=False)

    def run(self, flow_name, node_args=None, parent=None, retried_count=None, retry=None,
            state=None, selective=False, migration_version=None):
        # pylint: disable=too-many-arguments,arguments-differ,too-many-locals
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
            'migration_version': migration_version or 0
        }

        Trace.log(Trace.DISPATCHER_WAKEUP, flow_info)

        # Perform migrations at first place
        self.migrate_message(flow_info)

        try:
            system_state = SystemState(self.request.id, flow_name, node_args, retry, state, parent, selective)
            retry = system_state.update()
        except FlowError as exc:
            max_retry = Config.max_retry.get(flow_name, 0)
            Trace.log(Trace.FLOW_FAILURE, flow_info, state=exc.state, will_retry=retried_count < max_retry)
            raise self.selinon_retry(
                flow_info=flow_info,
                adjust_retried_count=True,
                keep_state=False
            )
        except DispatcherRetry as exc:
            raise self.selinon_retry(flow_info, exc.adjust_retry_count, keep_state=exc.keep_state)
        except Exception:
            Trace.log(Trace.DISPATCHER_FAILURE, flow_info, what=traceback.format_exc())
            raise self.flow_failure(state)

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
                'selective': system_state.selective,
                'migration_version': flow_info['migration_version']
            }
            Trace.log(Trace.DISPATCHER_RETRY, flow_info, kwargs)
            raise self.retry(args=[], kwargs=kwargs, countdown=retry, queue=Config.dispatcher_queues[flow_name])

        Trace.log(Trace.FLOW_END, flow_info, state=state_dict)
        return {
            'finished_nodes': state_dict['finished_nodes'],
            # This is always {} since we have finished, but leave it here because of failure tracking.
            'failed_nodes': state_dict['failed_nodes'],
            # Always an empty array.
            'active_nodes': state_dict.get('active_nodes', [])
        }
