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

import os
import runpy
import tempfile
from .trace import Trace
from selinonlib import System


class Config(object):
    """
    All user configurations generated from YAML file
    """
    celery_app = None

    get_task_instance = None
    is_flow = None
    edge_table = None
    failures = None
    nowait_nodes = None
    max_retry = None
    retry_countdown = None
    storage_readonly = None
    propagate_node_args = None
    propagate_finished = None
    propagate_parent = None
    propagate_compound_finished = None
    propagate_compound_parent = None
    output_schemas = None

    storage_mapping = None
    task_mapping = None
    dispatcher_queue = None
    task_queues = None
    strategy_function = None


    @classmethod
    def _set_config(cls, config_module):
        """
        Set configuration from Python's module
        """
        cls.get_task_instance = config_module['get_task_instance']
        cls.is_flow = config_module['is_flow']
        cls.edge_table = config_module['edge_table']
        cls.failures = config_module['failures']
        cls.nowait_nodes = config_module['nowait_nodes']

        # propagate_* entries
        cls.propagate_finished = config_module['propagate_finished']
        cls.propagate_node_args = config_module['propagate_node_args']
        cls.propagate_parent = config_module['propagate_parent']
        cls.propagate_compound_finished = config_module['propagate_compound_finished']
        cls.propagate_compound_parent = config_module['propagate_compound_parent']

        # task configuration
        cls.output_schemas = config_module['output_schemas']
        cls.storage_mapping = config_module['storage2instance_mapping']
        cls.task_mapping = config_module['task2storage_mapping']
        cls.max_retry = config_module['max_retry']
        cls.retry_countdown = config_module['retry_countdown']
        cls.storage_readonly = config_module['storage_readonly']

        # queues
        cls.dispatcher_queue = config_module['dispatcher_queue']
        cls.task_queues = config_module['task_queues']

        # Dispatcher scheduling strategy
        cls.strategy_function = config_module['strategy_function']

        # call config init with Config class to set up other configuration specific values
        config_module['init'](cls)

    @classmethod
    def set_config_py(cls, config_code):
        """
        Set dispatcher configuration by Python config file

        :param config_code: configuration source code
        """
        config_module = runpy.run_path(config_code)
        cls._set_config(config_module)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files, config_py=None):
        """
        Set dispatcher configuration by path to YAML configuration files

        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        :param config_py: a file that should be used for storing generated config.py
        """
        system = System.from_files(nodes_definition_file, flow_definition_files)

        if not config_py:
            tmp_f = tempfile.NamedTemporaryFile(mode="w", delete=False)
            system.dump2stream(tmp_f)
            tmp_f.close()
            cls.set_config_py(tmp_f.name)
            os.unlink(tmp_f.name)
        else:
            with open(config_py, "w") as f:
                system.dump2stream(f)
            cls.set_config_py(config_py)

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

    @classmethod
    def set_celery_app(cls, celery_app):
        """
        Set celery application that should be used

        :param celery_app: celery app instance
        """
        # Avoid circular imports
        from .dispatcher import Dispatcher
        from .selinonTaskEnvelope import SelinonTaskEnvelope

        cls.celery_app = celery_app
        celery_app.tasks.register(Dispatcher())
        celery_app.tasks.register(SelinonTaskEnvelope())

