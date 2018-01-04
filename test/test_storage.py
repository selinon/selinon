#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinon_test_case import SelinonTestCase

from selinon import SystemState
from selinon import DataStorage
from selinon.config import Config


class TestStorageAccess(SelinonTestCase):
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
            def __init__(self):
                pass

            def connect(self):
                # shouldn't be called
                raise NotImplementedError()

            def disconnect(self):
                # called on destruction
                pass

            def is_connected(self):
                # return True so we can test retrieve()
                return True

            def store(self, node_args, flow_name, task_name, task_id, result):
                # shouldn't be called
                raise NotImplementedError()

            def retrieve(self, flow_name, task_name, task_id):
                assert flow_name == 'flow1'
                assert task_name == 'Task1'
                assert task_id == task1.task_id
                return 0xDEADBEEF

        def _cond_access(db, node_args):
            return db.get('Task1') == 0xDEADBEEF

        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_access},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        # Task1 has finished, we should access the database
        task1 = self.get_task('Task1')
        self.set_finished(task1, 1)

        Config.storage_mapping = {'Storage1': MyStorage()}
        Config.task2storage_mapping = {'Task1': 'Storage1'}

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        system_state.update()

        assert 'Task2' in self.instantiated_tasks

    @pytest.mark.skip(reason="store() currently not tested")
    def test_store(self):
        # store() is called transparently by SelinonTask - not possible to directly check it without running a task
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
            def __init__(self):
                pass

            def connect(self):
                raise ConnectionError()

            def disconnect(self):
                # called on destruction
                pass

            def is_connected(self):
                # return False so we can test connect()
                return False

            def store(self, node_args, flow_name, task_name, task_id, result):
                # shouldn't be called
                raise NotImplementedError()

            def retrieve(self, flow_name, task_name, task_id):
                # shouldn't be called
                raise NotImplementedError()

        def _cond_access(db, node_args):
            return db.get('Task1') == 0xDEADBEEF

        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_access},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        # Task1 has finished, we should access the database
        task1 = self.get_task('Task1')
        self.set_finished(task1, 1)

        Config.storage_mapping = {'Storage1': MyStorage()}
        Config.task2storage_mapping = {'Task1': 'Storage1'}

        with pytest.raises(ConnectionError):
            system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
            system_state.update()
