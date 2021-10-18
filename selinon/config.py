#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""All user configurations generated from YAML file."""

import io
import logging
import os
import runpy
import tempfile

from .errors import ConfigNotInitializedError
from .errors import ConfigurationError
from .errors import UnknownStorageError
from .system import System
from .trace import Trace


def requires_initialization(func):
    """Check that method that requires config can access initialized configuration."""
    def wrapper(class_, *args, **kwargs):
        """Wrap call for checking initialization."""
        if not class_.initialized:
            raise ConfigNotInitializedError("Selinon not initialized, cannot access configuration attributes")

        return func(class_, *args, **kwargs)

    return wrapper


class Config:
    """All user configurations generated from YAML file."""

    _logger = logging.getLogger(__name__)

    celery_app = None

    flows = {}
    task_classes = {}
    edge_table = {}
    nowait_nodes = {}
    eager_failures = None
    max_retry = None
    retry_countdown = None
    storage2storage_cache = {}
    storage_readonly = {}
    storage_task_name = {}
    propagate_node_args = {}
    propagate_parent = {}
    propagate_finished = {}
    propagate_compound_finished = {}
    output_schemas = None
    async_result_cache = {}
    migration_dir = None

    storage_mapping = {}
    task2storage_mapping = {}
    dispatcher_queues = {}
    task_queues = {}
    strategies = {}

    # Called from generated python code to mark that the configuration was correctly set up
    initialized = False

    @classmethod
    def _set_config(cls, config_module):
        """Set configuration from Python's module.

        :param config_module: config module to be used for attributes assignment
        """
        cls.task_classes = config_module['task_classes']
        cls.edge_table = config_module['edge_table']
        cls.failures = config_module['failures']
        cls.nowait_nodes = config_module['nowait_nodes']
        cls.eager_failures = config_module['eager_failures']
        cls.flows = list(cls.edge_table.keys())

        # misc
        cls.node_args_from_first = config_module['node_args_from_first']
        cls.async_result_cache = config_module['async_result_cache']

        # propagate_* entries
        cls.propagate_finished = config_module['propagate_finished']
        cls.propagate_node_args = config_module['propagate_node_args']
        cls.propagate_parent = config_module['propagate_parent']
        cls.propagate_compound_finished = config_module['propagate_compound_finished']

        # task configuration
        cls.output_schemas = config_module['output_schemas']
        cls.storage_mapping = config_module['storage2instance_mapping']
        cls.storage_task_name = config_module['storage_task_name']
        cls.task2storage_mapping = config_module['task2storage_mapping']
        cls.max_retry = config_module['max_retry']
        cls.retry_countdown = config_module['retry_countdown']
        cls.storage_readonly = config_module['storage_readonly']
        cls.storage2storage_cache = config_module['storage2storage_cache']

        # throttle configuration
        cls.throttle_tasks = config_module['throttle_tasks']
        cls.throttle_flows = config_module['throttle_flows']

        # queues
        cls.dispatcher_queues = config_module['dispatcher_queues']
        cls.task_queues = config_module['task_queues']

        # Dispatcher scheduling strategy
        cls.strategies = config_module['strategies']

        # Selective task configuration
        cls.selective_run_task = config_module['selective_run_task']

        # Configuration migrations
        cls.migration_dir = config_module['migration_dir']

        # call config init with Config class to set up other configuration specific values
        config_module['init'](cls)

    @classmethod
    def set_config_py(cls, config_code):
        """Set dispatcher configuration by Python config file.

        :param config_code: configuration source code
        """
        cls._logger.debug("Using config.py file from '%s'", config_code)
        config_module = runpy.run_path(config_code)
        cls._set_config(config_module)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files, config_py=None, keep_config_py=False):
        """Set dispatcher configuration by path to YAML configuration files.

        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        :param config_py: a file that should be used for storing generated config.py
        :param keep_config_py: do not remove config_py file after run
        """
        system = System.from_files(nodes_definition_file, flow_definition_files)

        if not config_py:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_f:
                cls._logger.debug("Generating config.py file to created temporary file '%s'", tmp_f.name)
                system.dump2stream(tmp_f)
            config_py = tmp_f.name
        else:
            cls._logger.debug("Generating config.py file to proposed file '%s'", config_py)
            with open(config_py, "w") as output_f:
                system.dump2stream(output_f)

        cls.set_config_py(config_py)

        if not keep_config_py:
            cls._logger.debug("Removing generated config.py file '%s'", config_py)
            os.unlink(config_py)

    @classmethod
    def set_config_dict(cls, nodes_definition, flow_definitions):
        """Set configuration using dictionaries, no files are written to filesystem.

        :param nodes_definition: definition of nodes in the system
        :type nodes_definition: dict
        :param flow_definitions: a list with flow defintions
        :type flow_definitions: list
        """
        system = System.from_dict(nodes_definition, flow_definitions)
        dump = io.StringIO()
        system.dump2stream(dump)
        conf = {}
        # Ignore B102
        exec(dump.getvalue(), conf)  # pylint: disable=exec-used
        cls._set_config(conf)

    @classmethod
    def trace_by_func(cls, trace_func):
        """Set tracing function for Dispatcher.

        :param trace_func: a function that should be used to trace dispatcher actions
        """
        Trace.trace_by_func(trace_func)

    @classmethod
    def trace_by_logging(cls):
        """Use Python's logging for tracing."""
        Trace.trace_by_logging()

    @classmethod
    def trace_by_sentry(cls, dsn=None):
        """Use Sentry for tracing.

        :param dsn: DSN for sentry to be used (uses env variables if omitted, see Sentry docs)
        """
        Trace.trace_by_sentry(dsn)

    @classmethod
    def trace_by_json(cls):
        """Trace directly JSON output."""
        Trace.trace_by_json()

    @classmethod
    def set_celery_app(cls, celery_app):
        """Set celery application that should be used.

        :param celery_app: celery app instance
        """
        # Avoid circular imports
        from .dispatcher import Dispatcher
        from .task_envelope import SelinonTaskEnvelope

        cls._logger.debug("Registering Selinon to Celery context")

        cls.celery_app = celery_app
        celery_app.tasks.register(Dispatcher())
        celery_app.tasks.register(SelinonTaskEnvelope())

    @classmethod
    def init(cls, celery_app, nodes_definition_file, flow_definition_files, config_py=None, keep_config_py=False):
        """Initialize Selinon configuration with Celery application.

        :param celery_app: celery application to be used
        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        :param config_py: a file that should be used for storing generated config.py
        :param keep_config_py: do not remove config_py file after run
        """
        # pylint: disable=too-many-arguments
        cls.set_config_yaml(nodes_definition_file, flow_definition_files, config_py, keep_config_py)
        cls.set_celery_app(celery_app)

    @staticmethod
    def _should_config(node_name, dst_node_name, configuration):
        """Syntax sugar for configuration entries that accept lists/booleans.

        :param node_name: node name
        :param dst_node_name: destination node to which configuration should be propagated
        :param configuration: configuration that should be checked
        :return: true if node_name satisfies configuration
        """
        if configuration[node_name] is True:
            return True

        if isinstance(configuration[node_name], list):
            return dst_node_name in configuration[node_name]

        return False

    @classmethod
    @requires_initialization
    def should_propagate_finished(cls, node_name, dst_node_name):
        """Check whether finished nodes should be propagated.

        :param node_name: node name that should be checked for propagate_finished
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_finish
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_finished)

    @classmethod
    @requires_initialization
    def should_propagate_node_args(cls, node_name, dst_node_name):
        """Check whether node arguments should be propagated.

        :param node_name: node name that should be checked for propagate_node_args
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_node_args
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_node_args)

    @classmethod
    @requires_initialization
    def should_propagate_parent(cls, node_name, dst_node_name):
        """Check whether parents be propagated.

        :param node_name: node name that should be checked for propagate_parent
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_parent
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_parent)

    @classmethod
    @requires_initialization
    def should_propagate_compound_finished(cls, node_name, dst_node_name):  # pylint: disable=invalid-name
        """Check whether finished should be propagated (compound/flattered mode).

        :param node_name: node name that should be checked for propagate_compound_finished
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_compound_finished
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_compound_finished)

    @classmethod
    @requires_initialization
    def get_task_instance(cls, task_name, flow_name, parent, task_id, dispatcher_id):
        """Get instance of SelinonTask.

        :param task_name: task name that should be instantiated (it is not necessarily SelinonTask)
        :param flow_name: flow name
        :param parent: parent nodes for instance
        :param task_id: id of the task
        :param dispatcher_id: id of dispatcher for flow flow
        :return: instance of the task
        """
        # pylint: disable=too-many-arguments
        task_class = Config.task_classes[task_name]
        return task_class(
            task_name=task_name,
            flow_name=flow_name,
            parent=parent,
            task_id=task_id,
            dispatcher_id=dispatcher_id
        )

    @classmethod
    @requires_initialization
    def is_flow(cls, node_name):
        """Check if given node is a flow by its name.

        :param node_name: name of the node to be checked
        :return: True if given node is a flow
        """
        return node_name in cls.flows

    @classmethod
    @requires_initialization
    def is_task(cls, node_name):
        """Check if given node is a task by its name.

        :param node_name: name of the node to be checked
        :return: True if given node is a task
        """
        return node_name in cls.task_classes

    @classmethod
    @requires_initialization
    def has_storage(cls, task_name):
        """Check whether the given task has assigned storage.

        :param task_name: name of a task
        :return: True if given task has assigned storage (either rw or readonly)
        """
        return task_name in cls.task2storage_mapping

    @classmethod
    @requires_initialization
    def has_readonly_storage(cls, task_name):
        """Check whether the given task has storage in read-only mode (results are not saved).

        :param task_name: name of a task
        :return: True if the given task has assigned readonly storage
        """
        return task_name in cls.storage_readonly

    @classmethod
    @requires_initialization
    def has_readwrite_storage(cls, task_name):
        """Check whether the given task has storage assigned and results are stored.

        :param task_name: name of a task
        :return: True if the given task has assigned rw storage
        """
        return cls.has_storage(task_name) and not cls.has_readonly_storage(task_name)

    @classmethod
    @requires_initialization
    def get_starting_edges(cls, flow_name):
        """Get starting edges for a flow.

        :param flow_name: a flow name to get starting edges
        :return: starting edges for a flow
        """
        if flow_name not in Config.edge_table:
            raise UnknownStorageError("No such flow in configuration: %s" % flow_name)

        start_edges = [(i, edge) for i, edge in enumerate(Config.edge_table[flow_name]) if len(edge['from']) == 0]
        if not start_edges:
            # This should not occur since selinon raises exception if such occurs, but just to be sure...
            raise ConfigurationError("No starting node found for flow '%s'!" % flow_name)

        return start_edges
