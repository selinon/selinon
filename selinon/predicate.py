#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Predicate interface - predicate for building conditions."""

import abc

import codegen

from .errors import ConfigurationError
from .helpers import check_conf_keys
from .helpers import dict2json


class Predicate(metaclass=abc.ABCMeta):
    """An abstract predicate representation."""

    @abc.abstractmethod
    def __init__(self):
        """Instantiate predicate representation."""

    @abc.abstractmethod
    def __str__(self):
        """Create a string representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """

    @abc.abstractclassmethod
    def create(cls, tree, nodes_from, flow, can_inspect_results):
        """Create the predicate.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes which are used within edge definition
        :type nodes_from: List[Nodes]
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: Predicate instance
        """

    @abc.abstractmethod
    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """

    @abc.abstractmethod
    def predicates_used(self):
        """Compute all predicates that are used (transitively) by child/children.

        :return: used predicates by children
        :rtype: List[Predicate]
        """

    @abc.abstractmethod
    def nodes_used(self):
        """Compute all nodes that are used (transitively) by child/children.

        :return: list of nodes that are used
        :rtype: List[Node]
        """

    @staticmethod
    def construct_default(flow):
        """Construct default predicate for edge.

        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :rtype: Predicate
        """
        from .builtin_predicate import AlwaysTruePredicate

        return AlwaysTruePredicate(flow=flow)

    @staticmethod
    def construct_default_dict():
        """Construct default predicate definition as it would be written in the configuration file."""
        return {'name': 'alwaysTrue'}

    @staticmethod
    def construct(tree, nodes_from, flow, can_inspect_results=True):  # pylint: disable=too-many-branches
        """Top-down creation of predicates - recursively called to construct predicates.

        :param tree: a dictionary describing nodes
        :type tree: dict
        :param nodes_from: nodes which are used within edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :rtype: Predicate
        """
        from .leaf_predicate import LeafPredicate
        from .builtin_predicate import OrPredicate, AndPredicate, NotPredicate

        if not tree:
            raise ConfigurationError("Bad condition '%s'" % tree)

        if 'name' in tree:
            if 'node' in tree:
                node = None
                for node_from in nodes_from:
                    if node_from.name == tree['node']:
                        node = node_from
                        break
                if node is None:
                    raise ConfigurationError("Node '%s' listed in predicate '%s' is not requested in 'nodes_from'"
                                             % (tree['node'], tree['name']))
            else:
                if len(nodes_from) == 1:
                    node = nodes_from[0]
                else:
                    # e.g. starting edge has no nodes_from
                    node = None

            unknown_conf = check_conf_keys(tree, known_conf_opts=('name', 'node', 'args'))
            if unknown_conf:
                raise ConfigurationError("Unknown configuration option for predicate '%s' in flow '%s': %s"
                                         % (tree['name'], flow.name, unknown_conf.keys()))

            predicate = LeafPredicate.create(tree['name'], node, flow, tree.get('args'))

            if not can_inspect_results and predicate.requires_message():
                raise ConfigurationError("Cannot inspect results of tasks '%s' in predicate '%s' in flow '%s'"
                                         % (nodes_from, tree['name'], flow.name))
            return predicate

        if 'or' in tree:
            return OrPredicate.create(tree['or'], nodes_from, flow, can_inspect_results)

        if 'not' in tree:
            return NotPredicate.create(tree['not'], nodes_from, flow, can_inspect_results)

        if 'and' in tree:
            return AndPredicate.create(tree['and'], nodes_from, flow, can_inspect_results)

        raise ConfigurationError("Unknown predicate:\n%s" % dict2json(tree))

    @staticmethod
    def construct_condition_name(flow_name, idx):
        """Create condition name for a dump.

        :param flow_name: flow name
        :type flow_name: str
        :param idx: index of condition within the flow
        :type idx: int
        :return: condition function representation
        """
        assert idx >= 0  # nosec
        return '_condition_{}_{}_cond'.format(flow_name, idx)

    def to_source(self):
        """Construct predicate source code.

        :return: predicate source code
        """
        return codegen.to_source(self.ast())

    @abc.abstractmethod
    def check(self):
        """Recursively/transitively check predicate correctness.

        :raises ValueError: if predicate is not correct
        """

    @abc.abstractmethod
    def requires_message(self):
        """Recursively check if one of the predicates require message from storage (result of previous task).

        :return: True if a result from storage is required
        """
