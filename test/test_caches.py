#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
import flexmock
from selinonTestCase import SelinonTestCase
from selinon import SystemState
from selinon import Cache
from selinon.errors import DispatcherRetry
from selinon.errors import CacheMissError
from selinon import DataStorage
from celery.result import AsyncResult


class TestCacheIssues(SelinonTestCase):
    class MyStorage(DataStorage):
        def __init__(self):
            self.data = None

        def connect(self):
            # shouldn't be called
            raise NotImplementedError()

        def disconnect(self):
            # called on destruction
            pass

        def is_connected(self):
            return True

        def store(self, node_args, flow_name, task_name, task_id, result):
            self.data = result

        def retrieve(self, flow_name, task_name, task_id):
            return self.data

    class MyCacheAddFailure(Cache):
        def add(self, item_id, item, task_name=None, flow_name=None):
            raise ValueError("Some error when adding to cache")

        def get(self, item_id, task_name=None, flow_name=None):
            raise CacheMissError("Result is not found in the cache")

    class MyCacheGetFailure(Cache):
        def add(self, item_id, item, task_name=None, flow_name=None):
            pass

        def get(self, item_id, task_name=None, flow_name=None):
            raise IndexError("Some error when getting values from cache")

    @pytest.mark.parametrize("cache", (MyCacheAddFailure(), MyCacheGetFailure()))
    def test_async_result_cache_issue(self, cache):
        #
        # flow1:
        #
        #     Task1
        #
        # This test ensures that async result cache issues do not affect flow behaviour.
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
        }
        self.init(edge_table, async_result_cache={'flow1': cache})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        # Nothing should be raised here even though the cache fails on get(), flow should continue
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is None

    @pytest.mark.parametrize("cache", (MyCacheAddFailure(), MyCacheGetFailure()))
    def test_result_cache_issue(self, cache):
        #
        # flow1:
        #
        #     Task1
        #
        # Checks that if there is an issue with retrieving task result from result cache in condition, dispatcher
        # will use directly storage.
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': 'Task2', 'condition': lambda db, _: db.get('Task1')}]
        }
        self.init(
            edge_table,
            storage_mapping={'Storage1': self.MyStorage()},
            task2storage_mapping={'Task1': 'Storage1'},
            storage2storage_cache={'Storage1': cache}
        )

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

