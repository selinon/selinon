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
from .storagePool import StoragePool
from .trace import Trace


class Dispatcher(Task):
    """
    Celeriac Dispatcher worker implementation
    """
    @classmethod
    def _set_config(cls, config_module):
        cls._get_task_instance = config_module['get_task_instance']
        cls._is_flow = config_module['is_flow']
        cls._edge_table = config_module['edge_table']
        cls._failures = config_module['failures']
        cls._output_schemas = config_module['output_schemas']
        cls._nowait_nodes = config_module['nowait_nodes']
        StoragePool.set_storage_mapping(config_module['storage2instance_mapping'])
        StoragePool.set_task_mapping(config_module['task2storage_mapping'])
        # we should call initialization explicitly
        config_module['init']()

    @classmethod
    def set_config_code(cls, config_code):
        """
        Set dispatcher configuration by source code string
        :param config_code: configuration source code
        """
        config_module = runpy.run_path(config_code)
        cls._set_config(config_module)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files):
        """
        Set dispatcher configuration by path to YAML configuration files
        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        """
        # TODO: add once Parsley will be available in pip; the implementation should be (not tested):
        # import tempfile
        # from parsley import System
        # system = System.from_files(nodes_definition_file, flow_definition_files, no_check=False)
        # # we could do this in memory, but runpy does not support this
        # tmp_file = tempfile.NamedTemporaryFile(mode="rw")
        # system.dump2stream(tmp_file)
        # cls.set_config_code(tmp_file.name)
        raise NotImplementedError()

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

    def run(self, flow_name, args=None, retry=None, state=None):
        """
        Dispatcher entry-point - run each time a dispatcher is scheduled
        :param flow_name: name of the flow
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
            system_state = SystemState(self.request.id,
                                       self._edge_table,
                                       self._failures,
                                       self._nowait_nodes,
                                       flow_name, args, retry, state)
            retry = system_state.update(self._get_task_instance, self._is_flow)
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
            self.retry(kwargs=kwargs, retry=retry)
        else:
            Trace.log(Trace.FLOW_END, {'flow_name': flow_name,
                                       'dispatcher_id': self.request.id,
                                       'finished_nodes': state_dict['finished_nodes']})
            return state_dict['finished_nodes']
