#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""
Selinon Dispatcher worker implementation
"""

import json
import traceback
from celery import Task
from .systemState import SystemState
from .errors import FlowError
from .trace import Trace
from .config import Config


class Dispatcher(Task):
    """
    Selinon Dispatcher worker implementation
    """
    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "selinon.Dispatcher"

    def selinon_retry(self, flow_name, node_args, parent, retried_count):
        """"Retry whole flow on failure if configured so, forget any progress done so far

        :param flow_name: name of the flow to be retried
        :param node_args: flow arguments
        :param parent: flow parents
        :param retried_count: number of retries already done
        :raises celery.Retry: always
        """
        kwargs = {
            'flow_name': flow_name,
            'node_args': node_args,
            'parent': parent,
            'retried_count': retried_count+1,
            # Set these to None so Selinon will properly start the flow again
            'retry': None,
            'state': None
        }
        countdown = Config.retry_countdown.get(flow_name, 0)

        # we will force max retries to 1 so we are always retried by Celery
        raise self.retry(kwargs=kwargs,
                         max_retries=1,
                         countdown=countdown,
                         queue=Config.dispatcher_queues[flow_name])

    def run(self, flow_name, node_args=None, parent=None, retried_count=None, retry=None, state=None):
        # pylint: disable=too-many-arguments,arguments-differ
        """
        Dispatcher entry-point - run each time a dispatcher is scheduled

        :param flow_name: name of the flow
        :param parent: flow parent nodes
        :param node_args: arguments for workers
        :param retried_count: number of Selinon retries done (not Celery retries)
        :param retry: last retry countdown
        :param state: the current system state
        :raises: FlowError
        """

        Trace.log(Trace.DISPATCHER_WAKEUP, {
            'flow_name': flow_name,
            'dispatcher_id': self.request.id,
            'node_args': node_args,
            'retry': retry,
            'queue': Config.dispatcher_queues[flow_name],
            'state': state
        })
        try:
            system_state = SystemState(self.request.id, flow_name, node_args, retry, state, parent)
            retry = system_state.update()
        except FlowError as flow_error:
            retried_count = retried_count or 0
            max_retry = Config.max_retry.get(flow_name, 0)
            Trace.log(Trace.FLOW_FAILURE, {
                'flow_name': flow_name,
                'dispatcher_id': self.request.id,
                'state': json.loads(str(flow_error)),
                'node_args': node_args,
                'parent': parent,
                'retry': retry,
                'will_retry': retried_count < max_retry,
                'queue': Config.dispatcher_queues[flow_name]
            })

            if retried_count < max_retry:
                raise self.selinon_retry(flow_name, node_args, parent, retried_count)
            else:
                # force max_retries to 0 so we are not scheduled and marked as FAILED
                raise self.retry(max_retries=0, exc=flow_error)
        except Exception as exc:
            Trace.log(Trace.DISPATCHER_FAILURE, {
                'flow_name': flow_name,
                'dispatcher_id': self.request.id,
                'node_args': node_args,
                'parent': parent,
                'queue': Config.dispatcher_queues[flow_name],
                'what': traceback.format_exc()
            })
            raise self.retry(max_retries=0, exc=exc)

        state_dict = system_state.to_dict()
        node_args = system_state.node_args

        if retry is not None and retry >= 0:
            kwargs = {
                'flow_name': flow_name,
                'node_args': node_args,
                'parent': parent,
                'retry': retry,
                'state': state_dict
            }
            Trace.log(Trace.DISPATCHER_RETRY, {
                'flow_name': flow_name,
                'dispatcher_id': self.request.id,
                'retry': retry,
                'state_dict': state_dict,
                'node_args': node_args,
                'queue': Config.dispatcher_queues[flow_name]
            })
            raise self.retry(args=[], kwargs=kwargs, countdown=retry, queue=Config.dispatcher_queues[flow_name])
        else:
            # TODO: make this more rich - same keys as FLOW_FAILURE
            Trace.log(Trace.FLOW_END, {
                'flow_name': flow_name,
                'node_args': node_args,
                'parent': parent,
                'dispatcher_id': self.request.id,
                'queue': Config.dispatcher_queues[flow_name],
                'finished_nodes': state_dict['finished_nodes']
            })
            return {
                'finished_nodes': state_dict['finished_nodes'],
                # this is always {} since we have finished, but leave it here because of failure tracking
                'failed_nodes': state_dict['failed_nodes']
            }
