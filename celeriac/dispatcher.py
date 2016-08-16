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
from .celeriacStorageTask import CeleriacStorageTask
from .systemState import SystemState
from .flowError import FlowError
from .storagePool import StoragePool


class Dispatcher(CeleriacStorageTask):
    """
    Celeriac Dispatcher worker implementation
    """
    @classmethod
    def _set_config(cls, config_module):
        cls._get_task_instance = config_module['get_task_instance']
        cls._is_flow = config_module['is_flow']
        cls._edge_table = config_module['edge_table']
        cls._output_schemas = config_module['output_schemas']
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
        # system = System.from_files(nodes_definition_file, flow_definition_files)
        # # perform checks so we are sure that system is OK
        # system.check()
        # # we could do this in memory, but runpy does not support this
        # tmp_file = tempfile.NamedTemporaryFile(mode="rw")
        # system.dump2stream(tmp_file)
        # cls.set_config_code(tmp_file.name)
        raise NotImplementedError()

    @classmethod
    def set_trace(cls, trace_func):
        """
        Set tracing function for Dispatcher
        :param trace_func: a function that should be used to trace dispatcher actions
        """
        # TODO: use trace in sources
        cls._trace = trace_func

    def run(self, flow_name, args=None, retry=None, state=None):
        """
        Dispatcher entry-point - run each time a dispatcher is scheduled
        :param flow_name: name of the flow
        :param args: arguments for workers
        :param retry: last retry countdown
        :param state: the current system state
        :raises: FlowError
        """
        try:
            system_state = SystemState(self._edge_table, flow_name, args, retry, state)
            retry = system_state.update(self._get_task_instance, self._is_flow)
        except FlowError as flow_error:
            # force max_retries to 0 so we are not scheduled and marked as FAILED
            raise self.retry(max_retries=0, exc=flow_error)

        if retry:
            kwargs = {
                'flow': flow_name,
                'args': system_state.node_args,
                'retry': retry,
                'state': system_state.to_dict()
            }
            self.retry(kwargs=kwargs, retry=retry)
