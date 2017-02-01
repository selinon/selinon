#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""
A pool that carries all database connections for workers
"""

from .errors import CacheMissError
from .config import Config
from .lockPool import LockPool
from .trace import Trace


class StoragePool(object):
    """
    A pool that carries all database connections for workers
    """
    _storage_pool_locks = LockPool()

    def __init__(self, id_mapping, flow_name):
        self._id_mapping = id_mapping or {}
        self._flow_name = flow_name

    @classmethod
    def get_storage_name_by_task_name(cls, task_name, graceful=False):
        """
        :param task_name: name of a task
        :param graceful: return None instead of raising an exception
        :return: storage name for task
        """
        storage = Config.task2storage_mapping.get(task_name)

        if storage is None and not graceful:
            raise KeyError("No storage for task '%s' defined" % task_name)

        return storage

    @classmethod
    def get_storage_by_task_name(cls, task_name):
        """
        :param task_name: task's name for which storage should be get
        :rtype: DataStorage
        """
        storage_name = cls.get_storage_name_by_task_name(task_name, graceful=True)
        if storage_name:
            return cls.get_connected_storage(storage_name)
        else:
            return None

    @classmethod
    def get_connected_storage(cls, storage_name):
        """
        Retrieve connected storage based by its name stated in configuration

        :param storage_name: name of storage
        :return: connected storage
        """
        # if this raises KeyError exception it means that the flow was not configured properly - should
        # be handled by Selinonlib
        storage = Config.storage_mapping[storage_name]

        if not storage.is_connected():
            with cls._storage_pool_locks.get_lock(storage):
                if not storage.is_connected():
                    Trace.log(Trace.STORAGE_CONNECT, {'storage_name': storage_name})
                    storage.connect()

        return storage

    def get(self, task_name):
        """
        Retrieve data for task based on mapping for the current context

        :param task_name: task's name that we are retrieving data for
        :return: task's result for the current context
        """
        return self.retrieve(self._flow_name, task_name, self._id_mapping[task_name])

    @classmethod
    def retrieve(cls, flow_name, task_name, task_id):
        """
        Retrieve task's result from database which was configured to be used for desired task

        :param flow_name: flow in which the retrieval is taking place
        :param task_name: name of task for which result should be retrieved
        :param task_id: task ID to uniquely identify task results
        :return: task's result
        """
        storage = cls.get_storage_by_task_name(task_name)
        storage_task_name = Config.storage_task_name[task_name]
        storage_name = cls.get_storage_name_by_task_name(task_name)
        trace_msg = {
            'task_name': task_name,
            'storage_task_name': storage_task_name,
            'storage_name': storage_name,
            'flow_name': flow_name
        }

        with cls._storage_pool_locks.get_lock(storage):
            cache = Config.storage2storage_cache[storage_name]
            try:
                Trace.log(Trace.TASK_RESULT_CACHE_GET, trace_msg)
                result = cache.get(task_id, task_name=storage_task_name, flow_name=flow_name)
                Trace.log(Trace.TASK_RESULT_CACHE_HIT, trace_msg)
            except CacheMissError:
                Trace.log(Trace.TASK_RESULT_CACHE_MISS, trace_msg)
                Trace.log(Trace.STORAGE_RETRIEVE, trace_msg)
                result = storage.retrieve(flow_name, task_name, task_id)
                Trace.log(Trace.TASK_RESULT_CACHE_ADD, trace_msg)
                cache.add(task_id, result)

        return result

    @classmethod
    def set(cls, node_args, flow_name, task_name, task_id, result):
        # pylint: disable=too-many-arguments
        """
        Store result for task

        :param node_args: arguments that were passed to the node
        :param flow_name: flow in which task was run
        :param task_name: task that computed result
        :param task_id: task id that computed result
        :param result: result that should be stored
        :return: result ID - a unique ID which can be used to reference task results
        """
        storage = cls.get_storage_by_task_name(task_name)
        storage_task_name = Config.storage_task_name[task_name]

        record_id = storage.store(node_args, flow_name, storage_task_name, task_id, result)
        Trace.log(Trace.STORAGE_STORE, {
            'flow_name': flow_name,
            'node_args': node_args,
            'task_name': task_name,
            'storage_task_name': storage_task_name,
            'task_id': task_id,
            'storage_name': Config.task2storage_mapping[task_name],
            'result': result,
            'record_id': record_id
        })
        return record_id
