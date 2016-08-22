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

import unittest
from getTaskInstance import GetTaskInstance
from isFlow import IsFlow
from celery.result import AsyncResult
from celeriac import SystemState
from celeriac import DataStorage
from celeriac import StoragePool


def _cond_true(x):
    return True


class TestStorageAccess(unittest.TestCase):

    def setUp(self):
        AsyncResult.clear()
        GetTaskInstance.clear()
        StoragePool.set_storage_mapping({})

    def test_retrieve(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     Task2
        #
        class MyStorage(DataStorage):
            def connect(self):
                # shouldn't be called
                raise NotImplementedError()

            def disconnect(self):
                # shouldn't be called
                raise NotImplementedError()

            def connected(self):
                # return True so we can test retrieve()
                return True

            def store(self, flow_name, task_name, task_id, result):
                # shouldn't be called
                raise NotImplementedError()

            def retrieve(self, flow_name, task_name, task_id):
                assert(flow_name == 'flow1')
                assert(task_name == 'Task1')
                assert(task_id == task1.task_id)
                return 0xDEADBEEF

        def _cond_access(db):
            return db.get('flow1', 'Task1') == 0xDEADBEEF

        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_access},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}]
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])

        system_state = SystemState(edge_table, None, nowait_nodes, 'flow1')
        retry = system_state.update(get_task_instance, is_flow)
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)

        # Task1 has finished, we should access the database
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, 1)

        StoragePool.set_storage_mapping({'Storage1': MyStorage(None)})
        StoragePool.set_task_mapping({'Task1': 'Storage1'})

        system_state = SystemState(edge_table, None, nowait_nodes, 'flow1', state=state_dict, node_args=system_state.node_args)
        system_state.update(get_task_instance, is_flow)

        self.assertIn('Task2', get_task_instance.tasks)

    @unittest.skip("store() currently not tested")
    def test_store(self):
        # store() is called transparently by CeleriacTask - not possible to directly check it without running a task
        pass

    def test_connect_and_configuration(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     Task2
        #
        class MyStorage(DataStorage):
            def connect(self):
                # should be called, ensure that we have correct connection configuration
                assert(self.configuration == storage_config)
                raise ConnectionError

            def disconnect(self):
                # shouldn't be called
                raise NotImplementedError()

            def connected(self):
                # return False so we can test connect()
                return False

            def store(self, flow_name, task_name, task_id, result):
                # shouldn't be called
                raise NotImplementedError()

            def retrieve(self, flow_name, task_name, task_id):
                # shouldn't be called
                raise NotImplementedError()

        def _cond_access(db):
            return db.get('flow1', 'Task1') == 0xDEADBEEF

        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_access},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}]
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])

        system_state = SystemState(edge_table, None, nowait_nodes, 'flow1')
        retry = system_state.update(get_task_instance, is_flow)
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)

        # Task1 has finished, we should access the database
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, 1)

        storage_config = {'foo': 'bar'}
        StoragePool.set_storage_mapping({'Storage1': MyStorage(storage_config)})
        StoragePool.set_task_mapping({'Task1': 'Storage1'})

        with self.assertRaises(ConnectionError):
            system_state = SystemState(edge_table, None, nowait_nodes, 'flow1', state=state_dict, node_args=system_state.node_args)
            system_state.update(get_task_instance, is_flow)
