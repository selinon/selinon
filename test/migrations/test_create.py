#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Test creation and generation of migration files.

All inputs/outputs used in these sources are present at test/data/migration/input and test/data/migration/output
respectively. Methods have "empty" implementation as name of tests distinguish file location and test decorators
specify test case expectations (failure/success).
"""

import json
import os
import pytest
import shutil
import tempfile

from selinon.errors import MigrationNotNeeded
from selinon.migrations import Migrator
from selinon.migrations import TaintedFlowStrategy

from selinon_test_case import SelinonTestCase


def migration_creation_test(tainted_flow_strategy=None):
    """A migration test (successful) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            old_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=False)
            new_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=True)
            reference_output = test_instance.get_reference_test_output(test.__name__)

            migrator = Migrator(test_instance.migration_dir)
            migration_file_path = migrator.create_migration_file(
                old_config_files[0],
                old_config_files[1],
                new_config_files[0],
                new_config_files[1],
                tainted_flow_strategy or TaintedFlowStrategy.get_default_option()
            )

            test_instance.check_migration_match(migration_file_path, reference_output)
            test(test_instance)

        return wrapper

    return decorator


def migration_test_exception(exc, tainted_flow_strategy=None):
    """A migration test (exception) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            old_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=False)
            new_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=True)
            migrator = Migrator(test_instance.migration_dir)

            with pytest.raises(exc):
                migrator.create_migration_file(
                    old_config_files[0],
                    old_config_files[1],
                    new_config_files[0],
                    new_config_files[1],
                    tainted_flow_strategy or TaintedFlowStrategy.get_default_option()
                )

        return wrapper

    return decorator


class TestCreateMigration(SelinonTestCase):
    """Test creation and generation of coniguration migrations."""

    migration_dir = None

    def setup_method(self):
        """Set up migration test - create a temporary migration directory."""
        self.migration_dir = tempfile.mkdtemp(prefix='migration_dir_')

    def teardown_method(self):
        """Clear all temporary files present in the migration directory after each method."""
        shutil.rmtree(self.migration_dir)

    def get_test_config_files(self, test_name, new_config_files=True):
        """Get configuration files for specific test case - path respects test method naming.

        :param test_name: name of test that is going to be executed
        :param new_config_files: True if new config files should be listed
        :return: path to nodes yaml and flow configuration files
        :rtype: tuple
        """
        # Force structure of test data
        test_dir = os.path.join(
            self.DATA_DIR,
            'migrations',
            'input',
            'new' if new_config_files else 'old',
            test_name[len('test_'):]
        )

        nodes_yaml = os.path.join(test_dir, 'nodes.yaml')
        flows_yaml = []
        flows_yaml_path = os.path.join(os.path.join(test_dir, 'flows'))
        for flow_file in os.listdir(flows_yaml_path):
            if flow_file.endswith('.yaml'):
                flows_yaml.append(os.path.join(flows_yaml_path, flow_file))

        return nodes_yaml, flows_yaml

    def get_reference_test_output(self, test_name):
        """Get output for test - path naming respects test method naming."""
        return os.path.join(
            self.DATA_DIR,
            'migrations',
            'output',
            "{test_name}.json".format(test_name=test_name[len('test_'):])
        )

    @staticmethod
    def check_migration_match(migration_file_path, reference_migration_file_path):
        """Check that two migrations match and migration metadata precedence."""
        with open(migration_file_path, 'r') as migration_file:
            migration = json.load(migration_file)

        with open(reference_migration_file_path, 'r') as reference_migration_file:
            reference_migration = json.load(reference_migration_file)

        assert 'migration' in reference_migration
        assert 'migration' in migration

        assert set(reference_migration['migration'].keys()) == set(migration['migration'].keys())

        # we check only migration, metadata may vary
        for flow_name in migration['migration'].keys():
            # required fields
            assert 'tainted_edges' in migration['migration'][flow_name], \
                "Migration check failed in flow %r" % flow_name
            json.dumps(migration['migration'][flow_name]['tainted_edges'])
            assert set(reference_migration['migration'][flow_name]['tainted_edges'].keys()) \
                   == set(migration['migration'][flow_name]['tainted_edges'].keys()), \
                "Migration check failed in flow %r" % flow_name

            assert 'tainting_nodes' in migration['migration'][flow_name], \
                "Migration check failed in flow %r" % flow_name
            assert set(reference_migration['migration'][flow_name]['tainting_nodes']) \
                   == set(migration['migration'][flow_name]['tainting_nodes']), \
                "Migration check failed in flow %r" % flow_name

            assert 'translation' in migration['migration'][flow_name], \
                "Migration check failed in flow %r" % flow_name
            assert reference_migration['migration'][flow_name]['translation']\
                   == reference_migration['migration'][flow_name]['translation'], \
                "Migration check failed in flow %r" % flow_name

        assert 'tainted_flow_strategy' in reference_migration
        assert 'tainted_flow_strategy' in migration
        assert reference_migration['tainted_flow_strategy'] == migration['tainted_flow_strategy']
        assert migration['tainted_flow_strategy'] in TaintedFlowStrategy.get_option_names()

        # check for metadata precedence
        assert '_meta' in migration, "No metadata present in the output!"
        assert 'datetime' in migration['_meta']
        assert 'host' in migration['_meta']
        assert 'selinon_version' in migration['_meta']
        assert 'user' in migration['_meta']

    @migration_test_exception(MigrationNotNeeded)
    def test_no_change(self):
        """Test that if no change is done in configuration files, an exception is raised."""

    @migration_test_exception(MigrationNotNeeded)
    def test_add_to(self):
        """Test adding source node - this doesn't require any migrations to be present."""

    @migration_test_exception(MigrationNotNeeded)
    def test_remove_to(self):
        """Test removing source node - this doesn't require any migrations to be present."""

    @migration_test_exception(MigrationNotNeeded)
    def test_add_flow(self):
        """Test adding a new flow - this doesn't require any migrations to be present."""

    @migration_creation_test(TaintedFlowStrategy.IGNORE)
    def test_add_from(self):
        """Test adding source node - edge should be discarded (mapping to None)."""

    @migration_creation_test(TaintedFlowStrategy.IGNORE)
    def test_remove_from(self):
        """Test removing source node - edge should be discarded (mapping to None)."""

    @migration_creation_test(TaintedFlowStrategy.RETRY)
    def test_remove_edge(self):
        """Test removing whole edge definition.

        Edge should be discarded (mapping to None) and all subsequent indexes shifted.
        """

    @migration_creation_test(TaintedFlowStrategy.FAIL)
    def test_add_edge(self):
        """Test adding whole edge definition.

        Edge should be discarded (mapping to None) and all subsequent indexes shifted.
        """
