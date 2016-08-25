#!/usr/bin/env python
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

from celery import Task
from .systemState import SystemState
from .flowError import FlowError
from .trace import Trace


class Dispatcher(Task):
    """
    Celeriac Dispatcher worker implementation
    """
    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    name = "Dispatcher"

    def run(self, flow_name, args=None, parent=None, retry=None, state=None):
        """
        Dispatcher entry-point - run each time a dispatcher is scheduled
        :param flow_name: name of the flow
        :param parent: flow parent nodes
        :param args: arguments for workers
        :param retry: last retry countdown
        :param state: the current system state
        :raises: FlowError
        """

        Trace.log(Trace.DISPATCHER_WAKEUP, {'flow_name': flow_name,
                                            'dispatcher_id': self.request.id,
                                            'args': args,
                                            'retry': retry,
                                            'state': state})
        try:
            system_state = SystemState(self.request.id, flow_name, args, retry, state, parent)
            retry = system_state.update()
        except FlowError as flow_error:
            Trace.log(Trace.FLOW_FAILURE, {'flow_name': flow_name,
                                           'dispatcher_id': self.request.id,
                                           'what': str(flow_error)})
            # force max_retries to 0 so we are not scheduled and marked as FAILED
            raise self.retry(max_retries=0, exc=flow_error)
        except Exception as exc:
            Trace.log(Trace.DISPATCHER_FAILURE, {'flow_name': flow_name,
                                                 'dispatcher_id': self.request.id,
                                                 'what': str(exc)})
            raise

        state_dict = system_state.to_dict()
        node_args = system_state.node_args

        if retry:
            kwargs = {
                'flow': flow_name,
                'args': node_args,
                'retry': retry,
                'state': state_dict
            }
            Trace.log(Trace.DISPATCHER_RETRY, {'flow_name': flow_name,
                                               'dispatcher_id': self.request.id,
                                               'retry': retry,
                                               'state_dict': state_dict,
                                               'args': node_args
                                               })
            raise self.retry(kwargs=kwargs, retry=retry)
        else:
            Trace.log(Trace.FLOW_END, {'flow_name': flow_name,
                                       'dispatcher_id': self.request.id,
                                       'finished_nodes': state_dict['finished_nodes']})
            return state_dict['finished_nodes']
