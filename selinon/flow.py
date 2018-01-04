#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""A flow representation."""

import logging

from .cache_config import CacheConfig
from .edge import Edge
from .errors import ConfigurationError
from .failures import Failures
from .helpers import check_conf_keys
from .node import Node
from .strategy import Strategy


class Flow(Node):  # pylint: disable=too-many-instance-attributes
    """Flow representation."""

    _DEFAULT_MAX_RETRY = 0
    _DEFAULT_RETRY_COUNTDOWN = 0
    _logger = logging.getLogger(__name__)

    def __init__(self, name, **opts):
        """Initialize flow node representation.

        :param name: flow name
        :type name: str
        :param opts: additional flow options as provided in YAML configuration, see implementation for more details
        """
        super().__init__(name)

        self._logger.debug("Creating flow '%s'", name)
        self.edges = opts.pop('edges', [])
        self.failures = opts.pop('failures', None)
        self.nowait_nodes = opts.pop('nowait_nodes', [])
        self.node_args_from_first = opts.pop('node_args_from_dict', False)
        self.queue_name = self._expand_queue_name(opts.pop('queue', None))
        self.strategy = Strategy.from_dict(opts.pop('sampling', {}), self.name)

        self.propagate_node_args = opts.pop('propagate_node_args', False)
        self.propagate_parent = opts.pop('propagate_parent', False)
        self.propagate_parent_failures = opts.pop('propagate_parent_failures', False)
        self.propagate_finished = opts.pop('propagate_finished', False)
        self.propagate_compound_finished = opts.pop('propagate_compound_finished', False)
        self.propagate_failures = opts.pop('propagate_failures', False)
        self.propagate_compound_failures = opts.pop('propagate_compound_failures', False)
        self.throttling = self.parse_throttling(opts.pop('throttling', {}))
        self.cache_config = opts.pop('cache_config', CacheConfig.get_default(self.name))
        self.max_retry = opts.pop('max_retry', self._DEFAULT_MAX_RETRY)
        self.retry_countdown = opts.pop('retry_countdown', self._DEFAULT_RETRY_COUNTDOWN)
        self.eager_failures = opts.pop('eager_failures', [])

        # disjoint config options
        assert self.propagate_finished is not True and self.propagate_compound_finished is not True  # nosec
        assert self.propagate_failures is not True and self.propagate_compound_failures is not True  # nosec

        if opts:
            raise ConfigurationError("Unknown flow option provided for flow '%s': %s" % (name, opts))

    def _set_propagate(self, system, flow_def, propagate_type):
        """Parse propagate_node_args flag and adjust flow accordingly.

        :param system: system that is used
        :param flow_def: flow definition
        :param propagate_type: propagate flag type
        """
        ret = False

        if propagate_type in flow_def and flow_def[propagate_type] is not None:
            if not isinstance(flow_def[propagate_type], list) and \
                    not isinstance(flow_def[propagate_type], bool):
                flow_def[propagate_type] = [flow_def[propagate_type]]

            if isinstance(flow_def[propagate_type], list):
                ret = []
                for node_name in flow_def[propagate_type]:
                    node = system.flow_by_name(node_name)
                    ret.append(node)
            elif isinstance(flow_def[propagate_type], bool):
                ret = flow_def[propagate_type]
            else:
                raise ConfigurationError("Unknown value in '%s' in flow %s" % (self.name, propagate_type))

        return ret

    def _check_conf_keys(self, flow_def):
        """Check configuration keys so no unknown and unwanted configuration is supplied.

        :param flow_def: dictionary containing flow definition
        :raises: ValueError on wrong configuration
        """
        known_conf_keys = ('name', 'failures', 'nowait', 'cache', 'sampling', 'throttling', 'node_args_from_first',
                           'propagate_node_args', 'propagate_finished', 'propagate_parent', 'propagate_parent_failures',
                           'edges', 'propagate_compound_finished', 'queue', 'max_retry', 'retry_countdown',
                           'propagate_failures', 'propagate_compound_failures', 'eager_failures')

        unknown_conf = check_conf_keys(flow_def, known_conf_keys)
        if unknown_conf:
            raise ConfigurationError("Unknown configuration option for flow '%s' supplied: %s"
                                     % (self.name, unknown_conf))

    def parse_definition(self, flow_def, system):
        """Parse flow definition (fill flow attributes) from a dictionary.

        :param flow_def: dictionary containing flow definition
        :param system: system in which flow is defined
        """
        # pylint: disable=too-many-branches
        assert flow_def['name'] == self.name  # nosec
        self._check_conf_keys(flow_def)

        if self.edges:
            raise ConfigurationError("Multiple definitions of flow '%s'" % self.name)

        if 'edges' not in flow_def.keys() or not flow_def['edges'] or not isinstance(flow_def['edges'], list):
            raise ConfigurationError("No flow edges provided in flow %r" % self.name)

        for edge_def in flow_def['edges']:
            edge = Edge.from_dict(edge_def, system, self)
            self.add_edge(edge)

        if 'failures' in flow_def and flow_def['failures']:
            failures = Failures.construct(system, self, flow_def['failures'])
            self.failures = failures

        if 'nowait' in flow_def and flow_def['nowait'] is not None:
            if not isinstance(flow_def['nowait'], list):
                flow_def['nowait'] = [flow_def['nowait']]

            for node_name in flow_def['nowait']:
                node = system.node_by_name(node_name)
                self.add_nowait_node(node)

        if 'eager_failures' in flow_def and flow_def['eager_failures']:
            if not isinstance(flow_def['eager_failures'], list):
                flow_def['eager_failures'] = [flow_def['eager_failures']]

            for node_name in flow_def['eager_failures']:
                node = system.node_by_name(node_name)
                self.add_eager_failure(node)

        if 'cache' in flow_def:
            if not isinstance(flow_def['cache'], dict):
                raise ConfigurationError("Flow cache for flow '%s' should be a dict with configuration, "
                                         "got '%s' instead" % (self.name, flow_def['cache']))
            self.cache_config = CacheConfig.from_dict(flow_def['cache'], self.name)

        if 'sampling' in flow_def:
            self.strategy = Strategy.from_dict(flow_def.get('sampling'), self.name)

        self.throttling = self.parse_throttling(flow_def.pop('throttling', {}))
        self.node_args_from_first = flow_def.get('node_args_from_first', False)
        self.propagate_node_args = self._set_propagate(system, flow_def, 'propagate_node_args')
        self.propagate_parent = self._set_propagate(system, flow_def, 'propagate_parent')
        self.propagate_parent_failures = self._set_propagate(system, flow_def, 'propagate_parent_failures')
        self.propagate_finished = self._set_propagate(system, flow_def, 'propagate_finished')
        self.propagate_compound_finished = self._set_propagate(system, flow_def, 'propagate_compound_finished')
        self.propagate_failures = self._set_propagate(system, flow_def, 'propagate_failures')
        self.propagate_compound_failures = self._set_propagate(system, flow_def, 'propagate_compound_failures')
        self.queue_name = self._expand_queue_name(flow_def.get('queue'))
        self.max_retry = flow_def.get('max_retry', self._DEFAULT_MAX_RETRY)
        self.retry_countdown = flow_def.get('retry_countdown', self._DEFAULT_RETRY_COUNTDOWN)

    def add_edge(self, edge):
        """Add edge to this flow.

        :param edge: edge to be added
        :type edge: List[Edge]
        """
        self.edges.append(edge)

    def add_nowait_node(self, node):
        """Add edge to this flow, do not wait for node to finish.

        :param node: add a node that should be marked with nowait flag
        """
        self.nowait_nodes.append(node)

    def add_eager_failure(self, node):
        """Add a node to eager failure nodes listing.

        :param node: a node that should be marked as eager failure node
        """
        self.eager_failures.append(node)

    def all_nodes_from(self):
        """Compute all nodes that are stated in 'from' in edges section for this flow.

        :return: all source nodes in flow, excluding failures
        """
        all_nodes_from = set()

        for edge in self.edges:
            all_nodes_from |= set(edge.nodes_from)

        return list(all_nodes_from)

    def all_nodes_to(self):
        """Compute all nodes that are stated in 'to' in edges section for this flow.

        :return: all destination nodes in flow, excluding failures
        """
        all_nodes_to = set()

        for edge in self.edges:
            all_nodes_to |= set(edge.nodes_to)

        return list(all_nodes_to)

    def all_source_nodes(self):
        """Compute all source nodes for this flow.

        :return: all source nodes in flow, including failures
        """
        if self.failures:
            return list(set(self.all_nodes_from()) | set(self.failures.all_waiting_nodes()))

        return self.all_nodes_from()

    def all_destination_nodes(self):
        """Compute all destination nodes for this flow.

        :return: all destination nodes in flow, including failures
        """
        if self.failures:
            return list(set(self.all_nodes_to()) | set(self.failures.all_fallback_nodes()))

        return self.all_nodes_to()

    def all_used_nodes(self):
        """Get all used nodes in this flow, including failures.

        :return: all used nodes in flow
        """
        return list(set(self.all_destination_nodes()) | set(self.all_source_nodes()))

    @staticmethod
    def _should_config(dst_node_name, configuration):
        """Syntax sugar for configuration entries that accept list or boolean at the same time.

        :param dst_node_name: destination node to which configuration should be propagated
        :param configuration: configuration that should be checked
        :return: true if node_name satisfies configuration
        """
        if configuration is True:
            return True

        if isinstance(configuration, list):
            return dst_node_name in configuration

        return False

    def should_propagate_finished(self, dst_node_name):
        """Check whether this flow should propagate info about finished nodes.

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_finished
        """
        return self._should_config(dst_node_name, self.propagate_finished)

    def should_propagate_failures(self, dst_node_name):
        """Check whether this flow should propagate info about failed nodes.

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_failures
        """
        return self._should_config(dst_node_name, self.propagate_failures)

    def should_propagate_node_args(self, dst_node_name):
        """Check whether this flow should propagate flow arguments.

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_node_args
        """
        return self._should_config(dst_node_name, self.propagate_node_args)

    def should_propagate_parent(self, dst_node_name):
        """Check whether this flow should propagate parent nodes.

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_parent
        """
        return self._should_config(dst_node_name, self.propagate_parent)

    def should_propagate_parent_failures(self, dst_node_name):  # pylint: disable=invalid-name
        """Check whether this flow should propagate parent node failures.

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_parent_failures
        """
        return self._should_config(dst_node_name, self.propagate_parent_failures)

    def should_propagate_compound_finished(self, dst_node_name):  # pylint: disable=invalid-name
        """Check whether this flow should info about finished nodes (in compound/flattered mode).

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_compound_finished
        """
        return self._should_config(dst_node_name, self.propagate_compound_finished)

    def should_propagate_compound_failures(self, dst_node_name):  # pylint: disable=invalid-name
        """Check whether this flow should info about failures (in compound/flattered mode).

        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_compound_failures
        """
        return self._should_config(dst_node_name, self.propagate_compound_failures)
