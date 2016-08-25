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

from .config import Config
from .lockPool import LockPool
from .trace import Trace


class StoragePool(object):
    """
    A pool that carries all database connections for workers
    """
    def __init__(self, id_mapping=None):
        self._id_mapping = id_mapping if id_mapping else {}

    @classmethod
    def get_storage_name_by_task_name(cls, task_name, graceful=False):
        """
        :param task_name: name of a task
        :param graceful: return None instead of raising an exception
        :return: storage name for task
        """
        try:
            return Config.task_mapping[task_name]
        except KeyError:
            if graceful:
                return None
            else:
                raise

    @classmethod
    def get_storage_by_task_name(cls, task_name):
        return cls._connected_storage(cls.get_storage_name_by_task_name(task_name))

    @classmethod
    def _connected_storage(cls, storage_name):
        # if this raises KeyError exception it means that the flow was not configured properly - should
        # be handled by Parsley
        storage = Config.storage_mapping[storage_name]

        if not storage.connected():
            with LockPool.get_lock(storage):
                if not storage.connected():
                    Trace.log(Trace.STORAGE_CONNECT, {'storage_name': storage_name})
                    # TODO: we could optimize this by limiting number of active connections
                    storage.connect()

        return storage

    def get(self, flow_name, task_name):
        storage = self.get_storage_by_task_name(task_name)
        Trace.log(Trace.STORAGE_RETRIEVE, {'flow_name': flow_name,
                                           'task_name': task_name,
                                           'storage_name': Config.task_mapping[task_name]})
        return storage.retrieve(flow_name, task_name, self._id_mapping[task_name])

    @classmethod
    def set(cls, flow_name, task_name, task_id, result):
        storage = cls.get_storage_by_task_name(task_name)
        Trace.log(Trace.STORAGE_STORE, {'flow_name': flow_name,
                                        'task_name': task_name,
                                        'task_id': task_id,
                                        'storage_name': Config.task_mapping[task_name],
                                        'result': result})
        storage.store(flow_name, task_name, task_id, result)
