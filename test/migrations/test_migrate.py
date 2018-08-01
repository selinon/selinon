#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
import copy
import pytest
from selinon.migrations import Migrator
from selinon.errors import MigrationFlowFail
from selinon.errors import MigrationFlowRetry
from selinon.errors import MigrationSkew
from selinon_test_case import SelinonTestCase


def state_dict(**state_kwargs):
    """A helper to make tests more readable and bullet proof - convert kwargs to a dict."""
    # check required field
    if state_kwargs:
        if 'waiting_edges' not in state_kwargs:
            state_kwargs['waiting_edges'] = []

        if 'triggered_edges' not in state_kwargs:
            state_kwargs['triggered_edges'] = []

        if 'active_nodes' not in state_kwargs:
            state_kwargs['active_nodes'] = []

        if 'finished_nodes' not in state_kwargs:
            state_kwargs['finished_nodes'] = {}

        if 'failed_nodes' not in state_kwargs:
            state_kwargs['failed_nodes'] = {}

        unknown = set(state_kwargs.keys()) ^ {
            'active_nodes',
            'finished_nodes',
            'waiting_edges',
            'failed_nodes',
            'triggered_edges'
        }
        if unknown:
            raise ValueError("Unknown state dictionary configuration provided: %s" % unknown)

    return state_kwargs


def migrate_message_test(flow_name, migration_version, original_state):
    """A migration test (successful) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            migration_dir = test_instance.get_migration_dir(test.__name__)
            migrator = Migrator(migration_dir)
            migrated_state, new_migration_version, tainted = migrator.perform_migration(flow_name,
                                                                                        copy.deepcopy(original_state),
                                                                                        migration_version)
            test(test_instance, original_state, migrated_state, new_migration_version, tainted)
            test_instance.sanity_assert(original_state, migrated_state)

        return wrapper

    return decorator


def migrate_message_exception(exception, flow_name, migration_version, original_state):
    """A migration test (exception) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            migration_dir = test_instance.get_migration_dir(test.__name__)
            migrator = Migrator(migration_dir)

            with pytest.raises(exception) as exc_info:
                migrator.perform_migration(flow_name, original_state, migration_version)

            test(test_instance, exc_info)

        return wrapper

    return decorator


class TestPerformMigration(SelinonTestCase):
    """Test actual migration based on generated migrations."""

    def get_migration_dir(self, test_name):
        """Get configuration files for specific test case - path respects test method naming.

        :param test_name: name of test that is going to be executed
        :return: a path to test migration directory
        """
        # Force structure of test data
        return os.path.join(self.DATA_DIR, 'migrations', 'migration_dirs', test_name[len('test_'):])

    @staticmethod
    def sanity_assert(original_state, migrated_state):
        """Check that state stays untouched except waiting_edges."""
        (original_state or {}).pop('waiting_edges', None)
        (migrated_state or {}).pop('waiting_edges', None)
        assert original_state == migrated_state

    @migrate_message_test('flow1', 0, None)
    def test_zero_migration(self, original_state, migrated_state, new_migration_version, tainted):
        """Test when no migration is defined and message keeps migration of version 0."""
        assert original_state == migrated_state
        assert new_migration_version == 0
        assert tainted is False

    @migrate_message_test('flow1', None, None)
    def test_no_migration_version(self, original_state, migrated_state, new_migration_version, tainted):
        """Test no migration version in message - this can occur in case of new flow is scheduled."""
        assert original_state == migrated_state
        assert new_migration_version == 0
        assert tainted is False

    @migrate_message_test('flow1', 0, state_dict(waiting_edges=[1], triggered_edges=[0],
                                                 active_nodes=[{"name": "Task1", "id": "id1"}]))
    def test_one_migration(self, original_state, migrated_state, new_migration_version, tainted):
        """Test one single migration from version 0 to version 1."""
        original_state.pop('waiting_edges')
        assert migrated_state.pop('waiting_edges') == [2]
        assert migrated_state == original_state
        assert new_migration_version == 1
        assert tainted is False

    @migrate_message_test('flow1', None, None)
    def test_start_latest_migration(self, original_state, migrated_state, new_migration_version, tainted):
        """Test that if a flow starts and is picked by worker with some migration, this migration has to be tracked."""
        assert original_state == migrated_state
        assert new_migration_version == 1
        assert tainted is False

    @migrate_message_test('flow1', 1, state_dict(active_nodes=[{'name': 'Task2', 'id': 'id2'}],
                                                 waiting_edges=[1],
                                                 finished_nodes={'Task1': 'id1'}))
    def test_migration_chaining(self, original_state, migrated_state, new_migration_version, tainted):
        assert 'waiting_edges' in migrated_state
        # Edge 2 is added as it waits for Task2 to finish
        assert set(migrated_state.pop('waiting_edges')) == {1, 2}
        original_state.pop('waiting_edges')
        # The edge to Task3 is discarded but as Task2 is in progress, the edge to Task4 is added.
        assert migrated_state == original_state
        assert new_migration_version == 3
        assert tainted is False

    @migrate_message_test('flow1', 1, state_dict(active_nodes=[{'name': 'Task1', 'id': 'id1'}],
                                                 waiting_edges=[1]))
    def test_migration_chaining_no_change(self, original_state, migrated_state, new_migration_version, tainted):
        assert migrated_state == original_state
        assert new_migration_version == 3
        assert tainted is False

    @migrate_message_test('flow1', 0, state_dict(active_nodes=[{'name': 'Task3', 'id': 'id3'}],
                                                 waiting_edges=[1, 2],
                                                 finished_nodes={'Task1': ['id1'], 'Task2': ['id2']}))
    def test_migration_chaining_tainted(self, original_state, migrated_state, new_migration_version, tainted):
        # The second edge is removed in the first migration and later a new one to Task4 is introduced.
        assert migrated_state.pop('waiting_edges') == [1]
        original_state.pop('waiting_edges')
        assert migrated_state == original_state
        assert new_migration_version == 2
        assert tainted is True

    @migrate_message_exception(MigrationSkew, 'flow1', 0, state_dict(active_nodes=[{'name': 'Task1', 'id': 'id1'}]))
    def test_migration_skew(self, _):
        """Test signalizing migration skew - migration version is from future i.e. version file not present."""

    @migrate_message_exception(MigrationFlowRetry, 'flow1', 1,
                               state_dict(active_nodes=[{'name': 'Task3', 'id': 'id3'}],
                                          waiting_edges=[1, 2],
                                          finished_nodes={'Task1': ['id1'], 'Task2': ['id2']}))
    def test_migration_flow_retry(self, exc_info):
        """Check that the first migration causing failure does not allow to continue (tainting nodes)."""
        assert exc_info.value.migration_version == 2
        assert exc_info.value.latest_migration_version == 3
        assert exc_info.value.tainting_nodes == ["Task2"]
        assert exc_info.value.tainted_edge is None

    @migrate_message_exception(MigrationFlowRetry, 'flow1', 1,
                               state_dict(active_nodes=[{'name': 'Task3', 'id': 'id3'}],
                                          waiting_edges=[1, 2],
                                          finished_nodes={'Task1': ['id1'], 'Task2': ['id2']}))
    def test_migration_flow_retry_chained(self, exc_info):
        """The second migration in the chain should raise a failure (tainted edges)."""
        assert exc_info.value.migration_version == 2
        assert exc_info.value.latest_migration_version == 3
        assert exc_info.value.tainting_nodes == ['Task2']
        assert exc_info.value.tainted_edge is None

    @migrate_message_exception(MigrationFlowFail, 'flow1', 1,
                               state_dict(active_nodes=[{'name': 'Task3', 'id': 'id3'}],
                                          waiting_edges=[1, 2],
                                          finished_nodes={'Task1': ['id1'], 'Task2': ['id2']}))
    def test_migration_flow_fail(self, exc_info):
        """Check that the first migration causing failure does not allow to continue (tainting nodes)."""
        assert exc_info.value.migration_version == 2
        assert exc_info.value.latest_migration_version == 3
        assert exc_info.value.tainting_nodes == ["Task2"]
        assert exc_info.value.tainted_edge is None

    @migrate_message_exception(MigrationFlowFail, 'flow1', 1,
                               state_dict(active_nodes=[{'name': 'Task3', 'id': 'id3'}],
                                          waiting_edges=[1, 2],
                                          finished_nodes={'Task1': ['id1'], 'Task2': ['id2']}))
    def test_migration_flow_fail_chained(self, exc_info):
        """The second migration in the chain should raise a failure (tainted edges)."""
        assert exc_info.value.migration_version == 2
        assert exc_info.value.latest_migration_version == 3
        assert exc_info.value.tainting_nodes == ['Task2']
        assert exc_info.value.tainted_edge is None
