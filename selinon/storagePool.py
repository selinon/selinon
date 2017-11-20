#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""A pool that carries all database connections for workers."""

import traceback

from selinonlib import UnknownStorageError

from .config import Config
from .errors import CacheMissError
from .errors import StorageError
from .lockPool import LockPool
from .trace import Trace


class StoragePool(object):
    """A pool that carries all database connections for workers."""

    _storage_pool_locks = LockPool()

    def __init__(self, id_mapping, flow_name):
        """Initialize storage pool instance based on the current context.

        :param id_mapping: mapping tasks and their ids
        :param flow_name: name of flow for which StoragePool context is created
        """
        self._id_mapping = id_mapping or {}
        self._flow_name = flow_name

    @classmethod
    def get_storage_name_by_task_name(cls, task_name, graceful=False):
        """Get name of storage that was assigned to the given task.

        :param task_name: name of a task
        :param graceful: return None instead of raising an exception
        :return: storage name for task
        """
        storage = Config.task2storage_mapping.get(task_name)

        if storage is None and not graceful:
            raise UnknownStorageError("No storage for task '%s' defined" % task_name)

        return storage

    @classmethod
    def get_storage_by_task_name(cls, task_name):
        """Get storage instance that was assigned to the given task.

        :param task_name: task's name for which storage should be get
        :rtype: DataStorage
        """
        storage_name = cls.get_storage_name_by_task_name(task_name, graceful=True)

        if storage_name:
            return cls.get_connected_storage(storage_name)

        return None

    @classmethod
    def get_connected_storage(cls, storage_name):
        """Retrieve connected storage based by its name stated in configuration.

        :param storage_name: name of storage
        :return: connected storage
        """
        # if this raises UnknownStorageError exception it means that the flow was not configured properly - should
        # be handled by Selinonlib
        storage = Config.storage_mapping[storage_name]

        if not storage.is_connected():
            with cls._storage_pool_locks.get_lock(storage):
                if not storage.is_connected():
                    Trace.log(Trace.STORAGE_CONNECT, {'storage_name': storage_name})
                    storage.connect()

        return storage

    def get(self, task_name):
        """Retrieve data for task based on mapping for the current context.

        :param task_name: task's name that we are retrieving data for
        :return: task's result for the current context
        """
        return self.retrieve(self._flow_name, task_name, self._id_mapping[task_name])

    @classmethod
    def retrieve(cls, flow_name, task_name, task_id):
        """Retrieve task's result from database which was configured to be used for desired task.

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
            'flow_name': flow_name,
            'task_id': task_id
        }

        with cls._storage_pool_locks.get_lock(storage):
            cache = Config.storage2storage_cache[storage_name]

            result = None
            result_retrieved = False

            # Actually it is OK if there are some issues with task result cache - if there is some issue, just
            # report it in the tracing mechanism so users are aware of it and try to talk directly to storage
            # instead.
            Trace.log(Trace.TASK_RESULT_CACHE_GET, trace_msg)
            try:
                result = cache.get(task_id, task_name=storage_task_name, flow_name=flow_name)
                result_retrieved = True
            except CacheMissError:
                Trace.log(Trace.TASK_RESULT_CACHE_MISS, trace_msg, what=traceback.format_exc())
            except Exception:  # pylint: disable=broad-except
                Trace.log(Trace.TASK_RESULT_CACHE_ISSUE, trace_msg, what=traceback.format_exc())
            else:
                Trace.log(Trace.TASK_RESULT_CACHE_HIT, trace_msg)

            if not result_retrieved:
                Trace.log(Trace.STORAGE_RETRIEVE, trace_msg)
                try:
                    result = storage.retrieve(flow_name, task_name, task_id)
                except Exception as exc:
                    error_msg = "Failed to retrieve result from storage after the result was not found in cache"
                    Trace.log(Trace.STORAGE_ISSUE, trace_msg, what=traceback.format_exc())
                    raise StorageError(error_msg) from exc

            Trace.log(Trace.TASK_RESULT_CACHE_ADD, trace_msg)
            try:
                cache.add(task_id, result)
            except Exception:  # pylint: disable=broad-except
                Trace.log(Trace.TASK_RESULT_CACHE_ISSUE, trace_msg, what=traceback.format_exc())

            return result

    @classmethod
    def set(cls, node_args, flow_name, task_name, task_id, result):
        # pylint: disable=too-many-arguments
        """Store result for task.

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
            'record_id': record_id
        })
        return record_id

    @classmethod
    def set_error(cls, node_args, flow_name, task_name, task_id, exc_info):
        # pylint: disable=too-many-arguments
        """Store error information for task failure.

        :param node_args: arguments that were passed to the node
        :param flow_name: flow in which task was run
        :param task_name: task that computed result
        :param task_id: task id that computed result
        :param exc_info: information about exception - tuple (type, value, traceback) as returned by sys.exc_info()
        :return: true if error was stored in database - DataStorage.store_error() was called
        """
        storage = cls.get_storage_by_task_name(task_name)
        storage_task_name = Config.storage_task_name[task_name]

        try:
            record_id = storage.store_error(node_args, flow_name, storage_task_name, task_id, exc_info)
        except NotImplementedError:
            return False

        # TODO: move conversion to string to enhanced JSON handler and rather pass objects in Trace.log()
        Trace.log(Trace.STORAGE_STORE_ERROR, {
            'flow_name': flow_name,
            'node_args': node_args,
            'task_name': task_name,
            'storage_task_name': storage_task_name,
            'task_id': task_id,
            'storage_name': Config.task2storage_mapping[task_name],
            'error_type': str(exc_info[0]),
            'error_value': str(exc_info[1]),
            'error_traceback': "".join(traceback.format_tb(exc_info[2])),
            'record_id': record_id
        })

        return True
