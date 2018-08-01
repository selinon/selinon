#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
from get_task_instance import GetTaskInstance
from queue_mock import QueueMock
from strategy_mock import strategy_function
from storage_task_name_mock import StorageTaskNameMock

from celery.result import AsyncResult
from selinon.caches import LRU
from selinon.config import Config
from selinon.system_state import SystemState
from selinon.trace import Trace


class _ThrottleTasks:
    def __init__(self, is_flow, throttle_conf=None):
        self._is_flow = is_flow
        self._throttle_conf = throttle_conf or {}

    def __getitem__(self, item):
        if self._is_flow(item):
            raise KeyError("Unable to find throttle flow configuration in task throttle configuration")
        return self._throttle_conf.get(item)


class _AsyncResultCacheMock:
    def __init__(self, is_flow):
        self.is_flow = is_flow
        self.cache = LRU(max_cache_size=0)

    def __getitem__(self, item):
        if not self.is_flow(item):
            raise KeyError("Unable to subscribe for AsyncResult cache since '%s' is not a flow", item)
        return self.cache


class _TaskResultCacheMock:
    def __init__(self):
        self.cache = LRU(max_cache_size=0)

    def __getitem__(self, item):
        return self.cache


class _SelectiveRunFunctionMock:
    def __getitem__(self, item):
        def func(flow_name, node_name, node_args, task_names, storage_pool):
            # Always return None so we always execute task before the desired one
            return None
        return func


class SelinonTestCase:
    """
    Main class for Selinon testing
    """
    # dir for data files for tests
    DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

    def setup_method(self, method):
        """Clean up all class attributes from previous runs."""
        AsyncResult.clear()
        GetTaskInstance.clear()
        SystemState._throttled_tasks = {}
        SystemState._throttled_flows = {}
        # Make sure we restore tracing function in tests
        Trace._trace_functions = []

    def teardown_method(self, method):
        """Clean up resources and configuration after a test."""
        Config.initialized = False
        Config.migration_dir = None

    def init(self, edge_table, **kwargs):
        """
        :param edge_table: table dict as defined in YAML file containing edges
        :param kwargs: additional parameters for test configuration
        """
        Config.edge_table = edge_table
        flows = list(edge_table.keys())

        Config.flows = kwargs.pop('flows', flows)
        Config.nowait_nodes = kwargs.pop('nowait_nodes', dict.fromkeys(flows, []))
        Config.eager_failures = kwargs.pop('eager_failures', dict.fromkeys(flows, []))
        Config.get_task_instance = kwargs.pop('get_task_instance', GetTaskInstance())
        Config.failures = kwargs.pop('failures', {})
        Config.propagate_node_args = kwargs.pop('propagate_node_args', dict.fromkeys(flows, False))
        Config.propagate_parent = kwargs.pop('propagate_parent', dict.fromkeys(flows, False))
        Config.propagate_finished = kwargs.pop('propagate_finished', dict.fromkeys(flows, False))
        Config.propagate_compound_finished = kwargs.pop('propagate_compound_finished', dict.fromkeys(flows, False))
        Config.max_retry = kwargs.pop('max_retry', {})
        Config.retry_countdown = kwargs.pop('retry_countdown', {})
        Config.task_queues = kwargs.pop('task_queues', QueueMock())
        Config.dispatcher_queues = kwargs.pop('dispatcher_queues', QueueMock())
        Config.strategies = kwargs.pop('strategies', dict.fromkeys(flows, strategy_function))
        # TODO: this is currently unused as we do not have tests for store()
        Config.storage_readonly = kwargs.pop('storage_readonly', {})
        Config.storage_task_name = kwargs.pop('storage_task_name', StorageTaskNameMock())
        Config.task2storage_mapping = kwargs.pop('task2storage_mapping', {})
        Config.storage2storage_cache = kwargs.pop('storage2storage_cache', _TaskResultCacheMock())
        Config.node_args_from_first = kwargs.pop('node_args_from_first', dict.fromkeys(flows, False))
        Config.throttle_flows = kwargs.pop('throttle_flows', dict.fromkeys(flows, None))
        Config.throttle_tasks = kwargs.pop('throttle_tasks', _ThrottleTasks(Config.is_flow,
                                                                            kwargs.get('throttle_tasks_conf')))
        Config.storage_mapping = kwargs.pop('storage_mapping', {})
        Config.output_schemas = kwargs.pop('output_schemas', {})
        Config.async_result_cache = kwargs.pop('async_result_cache', _AsyncResultCacheMock(Config.is_flow))
        Config.selective_run_task = kwargs.pop('selective_run_task', _SelectiveRunFunctionMock())
        Config.initialized = True

        if kwargs:
            raise ValueError("Unknown config options provided: %s" % set(kwargs.keys()))

        self._update_edge_table()

    @staticmethod
    def _update_edge_table():
        """
        As edge table stores some metadata for debugging, let's fake them during testing
        """
        for key in Config.edge_table.keys():
            for entry in Config.edge_table[key]:
                entry['foreach_str'] = 'foreach_str'
                entry['condition_str'] = 'condition_str'

    @staticmethod
    def cond_true(db, node_args):
        """
        Condition for edge transition

        :return: always true
        """
        return True

    @staticmethod
    def cond_false(db, node_args):
        """
        Condition for edge transition

        :return: always false
        """
        return False

    @property
    def instantiated_tasks(self):
        """
        :return: list of all instantiated tasks
        """
        return Config.get_task_instance.tasks

    @property
    def instantiated_flows(self):
        """
        :return: list of all instantiated flows
        """
        return Config.get_task_instance.flows

    @staticmethod
    def set_finished(node, result=None):
        """
        Mark node (task or flow) as successfully finished

        :param node: node to mark as finished
        :param result: result of node, if None, result is not set
        """
        AsyncResult.set_finished(node.task_id)
        if result is not None:
            AsyncResult.set_result(node.task_id, result)

    @staticmethod
    def set_failed(node, exc=None):
        """
        Mark node (task or flow) as failed

        :param node: node to mark as failed
        :param result: exception (result for Celery) that was raised
        """
        AsyncResult.set_failed(node.task_id)
        AsyncResult.set_result(node.task_id, exc or ValueError("Some unexpected exception in node"))

    @staticmethod
    def get_task(task_name, idx=None):
        """
        Get task by its name

        :param task_name: name of task
        :param idx: index of task in instantiated tasks, if there are more tasks of type, the last is taken
        :return: task
        """
        tasks = Config.get_task_instance.task_by_name(task_name)
        return tasks[idx if idx is not None else -1]

    @staticmethod
    def get_all_tasks(task_name):
        """
        Get all tasks by task name

        :param task_name: name of task
        :return: tasks
        """
        return Config.get_task_instance.task_by_name(task_name)

    @staticmethod
    def get_flow(flow_name, idx=None):
        """
        Get flow by its name

        :param flow_name: name of flow
        :param idx: index of flow in instantiated flows, if there are more tasks of type, the last is taken
        :return: flow
        """
        tasks = Config.get_task_instance.flow_by_name(flow_name)
        return tasks[idx or -1]

    @staticmethod
    def get_all_flows(flow_name):
        """
        Get all flows by its name

        :param flow_name: name of flow
        :return: flows
        """
        return Config.get_task_instance.flow_by_name(flow_name)

    @property
    def get_task_instance(self):
        """
        :return: GetTaskInstance in the current context
        """
        return Config.get_task_instance

    @staticmethod
    def remove_all_flows_by_name(flow_name):
        """ Remove all flows with the given name """
        Config.get_task_instance.remove_all_flows_by_name(flow_name)

    @staticmethod
    def remove_all_tasks_by_name(task_name):
        """ Remove all flows with the given name """
        Config.get_task_instance.remove_all_tasks_by_name(task_name)
