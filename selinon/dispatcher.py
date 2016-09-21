#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import traceback
from celery import Task
from .systemState import SystemState
from .flowError import FlowError
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
    name = "Dispatcher"

    def run(self, flow_name, node_args=None, parent=None, finished=None, retry=None, state=None):
        """
        Dispatcher entry-point - run each time a dispatcher is scheduled

        :param flow_name: name of the flow
        :param parent: flow parent nodes
        :param node_args: arguments for workers
        :param finished: finished tasks in case of fallback is run, otherwise None
        :param retry: last retry countdown
        :param state: the current system state
        :raises: FlowError
        """

        Trace.log(Trace.DISPATCHER_WAKEUP, {'flow_name': flow_name,
                                            'dispatcher_id': self.request.id,
                                            'node_args': node_args,
                                            'finished': finished,
                                            'retry': retry,
                                            'state': state})
        try:
            system_state = SystemState(self.request.id, flow_name, node_args, retry, state, parent, finished)
            retry = system_state.update()
        except FlowError as flow_error:
            Trace.log(Trace.FLOW_FAILURE, {'flow_name': flow_name,
                                           'dispatcher_id': self.request.id,
                                           'state': state,
                                           'node_args': node_args,
                                           'parent': parent,
                                           'finished': finished,
                                           'retry': retry,
                                           'what': str(flow_error)})
            # force max_retries to 0 so we are not scheduled and marked as FAILED
            raise self.retry(max_retries=0, exc=flow_error)
        except Exception as exc:
            Trace.log(Trace.DISPATCHER_FAILURE, {'flow_name': flow_name,
                                                 'dispatcher_id': self.request.id,
                                                 'what': traceback.format_exc()})
            raise

        state_dict = system_state.to_dict()
        node_args = system_state.node_args

        if retry:
            kwargs = {
                'flow_name': flow_name,
                'node_args': node_args,
                'parent': parent,
                'retry': retry,
                'state': state_dict
            }
            Trace.log(Trace.DISPATCHER_RETRY, {'flow_name': flow_name,
                                               'dispatcher_id': self.request.id,
                                               'retry': retry,
                                               'state_dict': state_dict,
                                               'node_args': node_args
                                               })
            raise self.retry(args=[], kwargs=kwargs, countdown=retry, queue=Config.dispatcher_queue)
        else:
            Trace.log(Trace.FLOW_END, {'flow_name': flow_name,
                                       'dispatcher_id': self.request.id,
                                       'finished_nodes': state_dict['finished_nodes']})
            return {
                'finished_nodes': state_dict['finished_nodes'],
                # this is always {} since we have finished, but leave it here because of failure tracking
                'failed_nodes': state_dict['failed_nodes']
            }
