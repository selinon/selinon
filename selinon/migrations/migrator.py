#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Migration of configuration files."""

from datetime import datetime
import logging
import os
import platform

import yaml

from selinon.errors import MigrationFlowFail
from selinon.errors import MigrationFlowRetry
from selinon.errors import MigrationNotNeeded
from selinon.errors import MigrationSkew
from selinon.errors import RequestError
from selinon.errors import UnknownError
from selinon.helpers import dict2json
from selinon.predicate import Predicate

from .tainted_flow_strategy import TaintedFlowStrategy

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Migrator:
    """Main class for performing configuration file migrations."""

    def __init__(self, migration_dir=None):
        """Initialize migrator.

        :param migration_dir: a path to directory containing migration files
        """
        self.migration_dir = migration_dir
        self.old_nodes_definition = None
        self.old_flow_definitions = {}
        self.new_nodes_definition = None
        self.new_flow_definitions = {}

    def _report_diff_flow(self):
        """Report added and removed flows."""
        new_flows = set(self.new_nodes_definition.get('flows', [])) - set(self.old_nodes_definition.get('flows', []))
        if new_flows:
            _logger.info("Newly introduced flows in your configuration: %s", ", ".join(new_flows))
            for new_flow in new_flows:
                self.new_flow_definitions.pop(new_flow)

        removed_flows = set(self.old_nodes_definition.get('flows', [])) -\
            set(self.new_nodes_definition.get('flows', []))

        if removed_flows:
            _logger.info("Removed flows from your configuration: %s", ", ".join(removed_flows))
            for removed_flow in removed_flows:
                self.old_flow_definitions.pop(removed_flow)

    def _load_flows(self, flow_files, is_old_flow=True):
        """Load flow into instance attributes.

        :param flow_files: a list of paths to flow configuration files
        :type flow_files: list
        :param is_old_flow: true if loading old configuration
        :type is_old_flow: bool
        """
        for flow_file_path in flow_files:
            with open(flow_file_path, 'r') as flow_file:
                content = yaml.safe_load(flow_file)
                for flow in content['flow-definitions']:
                    # Let's store only edges as other configuration options do not affect messages that are
                    # accepted by dispatcher
                    entry = {
                        'edges': flow['edges'],
                        'file_path': flow_file_path
                    }

                    if is_old_flow:
                        self.old_flow_definitions[flow['name']] = entry
                    else:
                        self.new_flow_definitions[flow['name']] = entry

    def _get_latest_migration_version(self):
        """Get latest migration number based on migration files present in the migration directory.

        :return: latest migration number
        """
        latest_migration_number = 0
        for file_name in os.listdir(self.migration_dir):
            file_path = os.path.join(self.migration_dir, file_name)
            if not os.path.isfile(file_path) or not file_name.endswith('.json') or file_name[0] == '.':
                _logger.debug("Skipping %r, not a file nor JSON file (or hidden file)", file_path)
                continue

            migration_number = file_name[:-len('.json')]
            try:
                migration_number = int(migration_number)
            except ValueError as exc:
                raise MigrationSkew("Unable to parse previous migrations, file name %r does not correspond "
                                    "to migration file - migration files should be named numerically"
                                    % file_path, available_migration_version=None) from exc

            latest_migration_number = max(migration_number, latest_migration_number)

        return latest_migration_number

    @staticmethod
    def _migration_file_name(migration_version):
        """Create migration file name based on migration version."""
        return str(migration_version) + ".json"

    def _get_new_migration_file_name(self):
        """Generate a new migration file name.

        :return: a name of new migration file where migrations should be stored
        :rtype: str
        """
        if not os.path.isdir(self.migration_dir):
            _logger.info("Creating migration directory %r", self.migration_dir)
            try:
                os.mkdir(self.migration_dir)
            except Exception as exc:
                raise RuntimeError("Migration directory does not exist, unable to create a new directory: %s"
                                   % str(exc))

        return self._migration_file_name(self._get_latest_migration_version() + 1)

    @staticmethod
    def _get_migration_metadata():
        """Add metadata to migration content."""
        from selinon import selinon_version
        try:
            user = os.getlogin()
        except Exception:  # pylint: disable=broad-except
            # Travis CI fails here, but let's be tolerant in other cases as well
            user = None

        return {
            'selinon_version': selinon_version,
            'host': platform.node(),
            'datetime': str(datetime.utcnow()),
            'user': user
        }

    @staticmethod
    def _is_same_migration(migration1, migration2):
        """Check if two migrations match, migration attributes that do not have effect on migration are ignored."""
        if migration1.keys() != migration2.keys():
            return False

        for flow_name in migration1.keys():
            if migration1[flow_name]['tainting_nodes'] != migration2[flow_name]['tainting_nodes']:
                return False

            if set(migration1[flow_name]['tainted_edges'].keys()) != set(migration2[flow_name]['tainted_edges'].keys()):
                return False

            if migration1[flow_name]['translation'] != migration2[flow_name]['translation']:
                return False

        return True

    def _warn_on_same_migration(self, migration):
        """Warn if the newly created migration is same as the old one."""
        # This could be optimized - we can reuse the latest migration version retrieval from call from caller.
        last_migration_version = self._get_latest_migration_version()
        if not last_migration_version:
            return

        last_migration_path = os.path.join(self.migration_dir, self._migration_file_name(last_migration_version))
        with open(last_migration_path, 'r') as last_migration_file:
            content = yaml.safe_load(last_migration_file)

        if self._is_same_migration(content['migration'], migration):
            _logger.warning("Newly created migration is same as the old configuration. "
                            "Please make sure you don't run the same migration twice!")

    def _write_migration_file(self, migration, tainted_flow_strategy, add_meta):
        """Write computed migration to migration dir."""
        new_migration_file_name = self._get_new_migration_file_name()
        new_migration_file_path = os.path.join(self.migration_dir, new_migration_file_name)

        self._warn_on_same_migration(migration)

        migration_file_content = {
            'migration': migration,
            'tainted_flow_strategy': tainted_flow_strategy.name
        }

        if add_meta:
            migration_file_content['_meta'] = self._get_migration_metadata()

        with open(new_migration_file_path, 'w') as migration_file:
            migration_file.write(dict2json(migration_file_content))

        return new_migration_file_path

    @staticmethod
    def _preprocess_edges(edges):
        """Preprocess edges before computing migrations.

        :param edges: edges from the flow YAML configuration file to be preprocessed
        """
        for idx, edge in enumerate(edges):
            for key in list(edge.keys()):
                # For debug purposes - keep condition entry so it is written to the migration and tracing module can
                # report tainted edges correctly.
                if key not in ('from', 'to', 'condition'):
                    edge.pop(key)

            edge['_idx'] = idx
            # We don't care about node order
            if not isinstance(edge['from'], (list, set)):
                edge['from'] = [edge['from']] if edge['from'] is not None else []

            if not isinstance(edge['to'], (list, set)):
                edge['to'] = [edge['to']] if edge['to'] is not None else []

            edge['from'] = set(edge['from'])
            edge['to'] = set(edge['to'])

            if 'condition' not in edge:
                edge['condition'] = Predicate.construct_default_dict()

    def _calculate_flow_migration(self, old_flow_edges, new_flow_edges):
        """Calculate migration for a flow.

        :param old_flow_edges: edges definition of old flow
        :type old_flow_edges: dict
        :param new_flow_edges: edges definition of new flow
        :param new_flow_edges: dict
        :return: tuple describing migration, tainted edges and tainting edges
        :rtype: tuple
        """
        self._preprocess_edges(old_flow_edges)
        self._preprocess_edges(new_flow_edges)

        # Let's first construct subset of edges that was not affected by user's change
        for old_edge_idx, old_edge in enumerate(old_flow_edges):
            for new_edge_idx, new_edge in enumerate(new_flow_edges):
                if new_edge.get('_old_edge_idx') is not None:
                    continue

                if old_edge['from'] == new_edge['from'] and old_edge['to'] == new_edge['to']:
                    old_edge['_new_edge_idx'] = new_edge_idx
                    new_edge['_old_edge_idx'] = old_edge_idx
                    break

        _logger.debug("All old flow edges: %s", old_flow_edges)
        _logger.debug("All new flow edges: %s", new_flow_edges)

        old_unmatched = [edge for edge in old_flow_edges if '_new_edge_idx' not in edge.keys()]
        new_unmatched = [edge for edge in new_flow_edges if '_old_edge_idx' not in edge.keys()]

        _logger.debug("Old unmatched edges: %s", old_unmatched)
        _logger.debug("New unmatched edges: %s", new_unmatched)

        # Edges that were in the old flow, but they are not in the new one - if they run, it
        # means that the flow was tainted.
        # Also be consistent with tracing mechanism - use condition_str instead of condition key.
        tainted_edges = {str(edge['_idx']): {'from': edge['from'], 'to': edge['to'], 'condition_str': edge['condition']}
                         for edge in old_unmatched}

        # Source nodes of newly added edges would cause that the newly added edge would be executed. If these nodes
        # already run, the flow is tainted.
        tainting_nodes = {str(edge['_idx']): list(edge['from']) for edge in new_unmatched if edge['from']}

        translation = {}
        for old_edge in old_unmatched:
            matched = False
            for new_edge in new_unmatched:
                if old_edge['from'] == new_edge['from']:
                    translation[str(old_edge['_idx'])] = new_edge['_idx']
                    matched = True
                    break

            if not matched:
                translation[str(old_edge['_idx'])] = None

        # Keep track of edges that were remapped to another edge
        translation.update({str(edge['_idx']): edge.get('_new_edge_idx') for edge in old_flow_edges
                            if edge['_idx'] != edge.get('_new_edge_idx')})

        return translation, tainted_edges, tainting_nodes

    def _calculate_migrations(self, tainted_flow_strategy, add_meta):
        """Calculate migration of configuration files and store output in migration directory.

        :return: a path to newly created migration file in migration directory
        """
        migrations = {}
        for flow_name in self.old_flow_definitions:
            old_flow = self.old_flow_definitions[flow_name]
            new_flow = self.new_flow_definitions[flow_name]
            migration, tainted_edges, tainting_nodes = self._calculate_flow_migration(old_flow['edges'],
                                                                                      new_flow['edges'])

            if not migrations and not tainted_edges and not tainting_nodes:
                continue

            migrations[flow_name] = {
                'translation': migration,
                'tainted_edges': tainted_edges,
                'tainting_nodes': tainting_nodes
            }

        if not migrations:
            raise MigrationNotNeeded("No flow configuration changes that would require new migration detected")

        return self._write_migration_file(migrations, tainted_flow_strategy, add_meta)

    def create_migration_file(self, old_nodes_definition_path, old_flow_definitions_path,
                              new_nodes_definition_path, new_flow_definitions_path, tainted_flow_strategy,
                              add_meta=True):
        """Generate migration of configuration files, store output in the migration directory.

        :param old_nodes_definition_path: a path to old nodes.yaml
        :type old_nodes_definition_path: str
        :param old_flow_definitions_path: a list of paths to old flow definition files
        :type old_flow_definitions_path: list
        :param new_nodes_definition_path: a path to new nodes.yaml
        :type new_nodes_definition_path: str
        :param new_flow_definitions_path: a list of paths to new flow definition files
        :type new_flow_definitions_path: list
        :param tainted_flow_strategy: flow strategy for tainted flows
        :type tainted_flow_strategy: selinon.migrator.tainted_flow_strategy.TaintedFlowStrategy
        :param add_meta: add metadata information
        :type add_meta: bool
        :return: a path to newly created migration file
        """
        _logger.info("Performing configuration files migrations, storing results in %r", self.migration_dir)

        with open(old_nodes_definition_path, 'r') as old_nodes_definition_file:
            self.old_nodes_definition = yaml.safe_load(old_nodes_definition_file)

        self._load_flows(old_flow_definitions_path, is_old_flow=True)

        with open(new_nodes_definition_path, 'r') as new_nodes_definition_file:
            self.new_nodes_definition = yaml.safe_load(new_nodes_definition_file)

        old_migration_dir = self.old_nodes_definition.get('global', {}).get('migration_dir')
        new_migration_dir = self.new_nodes_definition.get('global', {}).get('migration_dir')

        if old_migration_dir != new_migration_dir and old_migration_dir is not None:
            _logger.warning("The old configuration points to migration directory located in %r whereas new "
                            "configuration points to migration directory located in %r. Make sure you have already "
                            "moved old migrations and they are present in %r already",
                            old_migration_dir, new_migration_dir, new_migration_dir)

        if self.migration_dir is None:
            self.migration_dir = new_migration_dir
            if new_migration_dir is None:
                raise RequestError("The migration directory not explicitly provided and cannot be determined from the "
                                   "nodes.yaml configuration (%r)" % new_nodes_definition_path)

        elif self.migration_dir != new_migration_dir:
            _logger.warning("Explicitly provided migration dir %r does not match migration dir present in the "
                            "nodes.yaml file (%r): %r. Make sure your migration gets correctly "
                            "propagated on deployment.",
                            self.migration_dir, new_nodes_definition_path, new_migration_dir)

        _logger.info("Calculating config file migrations, the computed migration will be placed to %r",
                     self.migration_dir)
        self._load_flows(new_flow_definitions_path, is_old_flow=False)
        self._report_diff_flow()
        return self._calculate_migrations(tainted_flow_strategy, add_meta)

    @staticmethod
    def _do_migration(migration_spec, flow_name, state, migration_version, latest_migration_version):
        """Do single migration of message based on migration definition.

        :param migration_spec: migration specification to be used
        :param flow_name: a name of flow for which the migration is perfomed
        :param state: the current flow state
        :param migration_version: the current migration version
        :param latest_migration_version: the latest migration version available on worker
        :return:
        """
        # pylint: disable=too-many-locals
        def raise_on_tainted_state(**kwargs):
            """Raise exceptions signalizing tainted flow when needed."""
            if tainted_flow_strategy == TaintedFlowStrategy.FAIL:
                raise MigrationFlowFail("Migration requested flow to fail",
                                        migration_version=migration_version,
                                        latest_migration_version=latest_migration_version,
                                        **kwargs)

            if tainted_flow_strategy == TaintedFlowStrategy.RETRY:
                raise MigrationFlowRetry("Migration requested flow to retry",
                                         migration_version=migration_version,
                                         latest_migration_version=latest_migration_version,
                                         **kwargs)

        flow_migration = migration_spec['migration'].get(flow_name)
        if not flow_migration:
            # Nothing to do, no changes in flow in the migration
            return state, False

        waiting_edges = state.get('waiting_edges', [])
        triggered_edges = state.get('triggered_edges', [])

        if not waiting_edges and not triggered_edges:
            return state, False

        tainted_flow_strategy = TaintedFlowStrategy.get_option_by_name(migration_spec['tainted_flow_strategy'])
        tainted = False
        for idx, triggered_edge in enumerate(triggered_edges):
            if str(triggered_edge) in flow_migration['tainted_edges'].keys():
                tainted = True
                raise_on_tainted_state(tainted_edge=flow_migration['tainted_edges'][str(triggered_edge)])

            # Make sure we track triggered edges with their new transcription based on migration
            if str(triggered_edge) in flow_migration['translation']:
                state['triggered_edge'][idx] = flow_migration['translation'][str(triggered_edge)]

        # Transcript also waiting edges so they are triggered correctly based on new configuration
        for idx, waiting_edge in enumerate(waiting_edges):
            if str(waiting_edge) in flow_migration['translation']:
                state['waiting_edges'][idx] = flow_migration['translation'][str(waiting_edge)]

        # Remove edges that should be discarded (mapped to None)
        state['waiting_edges'] = list(edge for edge in state['waiting_edges'] if edge is not None)
        state['triggered_edges'] = list(edge for edge in state['triggered_edges'] if edge is not None)

        finished_and_active_nodes = set(node_name for node_name in state['finished_nodes'].keys())
        for tainting_nodes in flow_migration['tainting_nodes'].values():
            if tainting_nodes and set(tainting_nodes).issubset(finished_and_active_nodes):
                tainted = True
                raise_on_tainted_state(tainting_nodes=tainting_nodes)

        # Add edges that were added and should be triggered after the corresponding subset of active nodes finish.
        active_nodes = {node['name'] for node in state['active_nodes']}
        for edge_idx, node_names in flow_migration['tainting_nodes'].items():
            edge_idx = int(edge_idx)
            if set(node_names).issubset(active_nodes) and edge_idx not in state['waiting_edges']:
                state['waiting_edges'].append(edge_idx)

        return state, tainted

    def perform_migration(self, flow_name, state, migration_version):
        """Perform actual migration based on message received.

        :param flow_name: name of flow for which the migration is performed
        :param state: state for which migration is present
        :param migration_version: migration version that was used previously
        :return: migrated message with current migration version and information about tainting flow
        :rtype: tuple
        """
        if not self.migration_dir:
            raise UnknownError("No migration directory provided on instantiation")

        latest_migration_version = self._get_latest_migration_version()
        if migration_version is None or state is None:
            # This means that this message is consumed for the first time, adjust migration based on migration
            # version of the current worker.
            return state, latest_migration_version, False

        if migration_version == latest_migration_version:
            _logger.debug("No migrations needed, migration version is %d", migration_version)
            return state, migration_version, False

        if migration_version > latest_migration_version:
            raise MigrationSkew("Received message with a newer migration number, "
                                "message migration version: %d, latest migration number present available: %d"
                                % (migration_version, latest_migration_version),
                                available_migration_version=latest_migration_version)

        tainted = False
        current_migration_version = migration_version
        while current_migration_version != latest_migration_version:
            current_migration_version += 1
            current_migration_path = os.path.join(self.migration_dir,
                                                  self._migration_file_name(current_migration_version))

            try:
                with open(current_migration_path, 'r') as migration_file:
                    migration_spec = yaml.safe_load(migration_file)
            except FileNotFoundError as exc:
                raise MigrationSkew("Migration file %r not found, cannot perform migrations"
                                    % current_migration_path,
                                    available_migration_version=current_migration_version) from exc

            state, taints_flow = self._do_migration(migration_spec,
                                                    flow_name,
                                                    state,
                                                    current_migration_version,
                                                    latest_migration_version)
            tainted = tainted or taints_flow

        return state, current_migration_version, tainted
