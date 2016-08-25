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

import runpy
from celery import Task
from .systemState import SystemState
from .flowError import FlowError
from .trace import Trace
from .config import Config


class Dispatcher(Task):
    """
    Celeriac Dispatcher worker implementation
    """
    @classmethod
    def set_config_py(cls, config_code):
        """
        Set dispatcher configuration by Python config file
        :param config_code: configuration source code
        """
        Config.set_config_py(config_code)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files):
        """
        Set dispatcher configuration by path to YAML configuration files
        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        """
        Config.set_config_yaml(nodes_definition_file, flow_definition_files)

    @classmethod
    def trace_by_func(cls, trace_func):
        """
        Set tracing function for Dispatcher
        :param trace_func: a function that should be used to trace dispatcher actions
        """
        Trace.trace_by_func(trace_func)

    @classmethod
    def trace_by_logging(cls):
        """
        Use Python's logging for tracing
        """
        Trace.trace_by_logging()

    def run(self, flow_name, parent, args=None, retry=None, state=None):
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
