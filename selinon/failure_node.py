#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
r"""Failure node handling representation.

A failure is basically a node in graph, that represents all permutations of possible cases of failures. Consider
having two failure conditions:

.. code-block:: yaml

  failures:
    - nodes:
          - Task1
          - Task2
          - Task3
      fallback:
          - FallbackTask1
    - nodes:
          - Task1
          - Task2
      fallback:
          - FallbackTask2

What we do here, we construct a graph of all possible permutations connected using edges that represent a node
that should be added to create a new permutation:

.. code-block:: yaml

   |-------------------------------
   |                              |
  T1           T2           T3    |
    \         /  \         / \    |
     \       /    \       /   \   |
      \     /      \     /     \  v
       T1,T2*       T2,T3      T1,T3
          \         /           /
           \       /           /
            T1,T2,T3*  <-------

For nodes ``T1,T2,T3`` and ``T1,T2`` we assign a fallback as configured. This graph is then serialized into the Python
configuration code. This way the dispatcher has O(N) time complexity when dealing with failures.

Note we are creating sparse tree - only for nodes that are listed in failures.

Note that we link failure nodes as allocated - we get a one way linked list of all failure nodes that helps us with
Python code generation.
"""

from functools import reduce

from .errors import ConfigurationError
from .predicate import Predicate


class FailureNode(object):
    """A representation of a failure node permutation."""

    def __init__(self, flow, traversed, failure_link):
        """Instantiate a failure node in the graph of all possible failures (permutations).

        :param flow: flow to which the failure node conforms to
        :param traversed: traversed nodes - also permutation of nodes
        :param failure_link: link to next failure node in failures
        """
        self.next = {}
        self.flow = flow
        self.traversed = traversed
        self.failure_link = failure_link
        self.fallbacks = []
        self.propagate_failures = []
        self.predicates = []

    def to(self, node_name):  # pylint: disable=invalid-name
        """Retrieve next permutation based on link in failure graph.

        :param node_name: a name of the node for next permutation
        :rtype: FailureNode
        """
        return self.next[node_name]

    def has_to(self, node_name):
        """Check whether there is a link to next permutation.

        :param node_name: name of the node to be added to create a new permutation
        :return: True if there is a link to next permutation for node of name node_name
        """
        return node_name in self.next

    def add_to(self, node_name, failure):
        """Add failure for next permutation.

        :param node_name: a node for next permutation
        :param failure: FailureNode that should be added
        """
        assert node_name not in self.next  # nosec
        self.next[node_name] = failure

    @staticmethod
    def _add_failure_info(failure_node, failure_info, predicate):
        """Add failure specific info to a failure node.

        :param failure_node: a failure node where the failure info should be added
        :param failure_info: additional information as passed from configuration file
        :param predicate: predicate that should be evaluated on a failure
        """
        if not isinstance(failure_info['fallback'], list) and failure_info['fallback'] is not True:
            failure_info['fallback'] = [failure_info['fallback']]

        # propagate_failure parsing
        if not isinstance(failure_info.get('propagate_failure', False), bool):
            raise ConfigurationError("Configuration option 'propagate_failure' for failure '%s' in flow '%s' "
                                     "should be boolean, got '%s' instead"
                                     % (failure_node.traversed, failure_node.flow.name,
                                        failure_info['propagate_failure']))

        if failure_info['fallback'] is True and failure_info.get('propagate_failure') is True:
            raise ConfigurationError("Configuration is misleading for failure '%s' in flow '%s' - cannot set "
                                     "propagate_failure and fallback to true at the same time"
                                     % (failure_node.traversed, failure_node.flow.name))

        failure_node.fallbacks.append(failure_info['fallback'])
        failure_node.predicates.append(predicate)
        failure_node.propagate_failures.append(failure_info.get('propagate_failure', False))

    @staticmethod
    def construct_condition_name(failure_node, idx):
        """Construct condition name that will be used in case of conditional fallbacks.

        :param failure_node: failure node for which the condition name should be constructed
        :param idx: index of condition that should be printed (could be multiple fallbacks with same source and dst)
        :return: string representation of constructed condition name as stated in the generated python code
        """
        return "_{flow}_{src}_f_{dest}_{idx}".format(flow=failure_node.flow.name,
                                                     src="_".join(failure_node.traversed),
                                                     dest="_".join(failure_node.fallbacks[idx]),
                                                     idx=idx)

    @classmethod
    def construct(cls, flow, system, failures):  # pylint: disable=too-many-locals,too-many-branches
        """Construct failures from failures dictionary.

        :param flow: flow to which failures conform to
        :param system: system context to be used
        :param failures: failures dictionary
        :return: a link for linked list of failures and a dict of starting failures
        """
        last_allocated = None
        starting_failures = {}
        predicates = []

        # pylint: disable=too-many-nested-blocks
        for failure in failures:
            used_starting_failures = {}

            for node in failure['nodes']:
                if node not in starting_failures:
                    failure_node = FailureNode(flow, [node], last_allocated)
                    last_allocated = failure_node
                    starting_failures[node] = failure_node
                    used_starting_failures[node] = failure_node
                else:
                    used_starting_failures[node] = starting_failures[node]

            current_nodes = list(used_starting_failures.values())
            next_nodes = []

            for _ in range(1, len(failure['nodes'])):  # for every permutation length

                for edge_node in failure['nodes']:  # edge_node is a node that can create a new permutation from a node

                    for current_node in current_nodes:

                        if edge_node not in current_node.traversed:

                            if not current_node.has_to(edge_node):
                                next_node = current_node.traversed + [edge_node]
                                failure_node = FailureNode(flow, next_node, last_allocated)
                                last_allocated = failure_node
                                current_node.add_to(edge_node, failure_node)

                                for node in current_nodes:
                                    diff = set(node.traversed) ^ set(next_node)

                                    if len(diff) == 1:
                                        if not node.has_to(list(diff)[0]):
                                            node.add_to(list(diff)[0], failure_node)

                                next_nodes.append(failure_node)
                            else:
                                # keep for generating new permutations
                                next_nodes.append(current_node.to(edge_node))

                current_nodes = next_nodes
                next_nodes = []

            failure_node = reduce(lambda x, y: x.to(y),
                                  failure['nodes'][1:],
                                  used_starting_failures[failure['nodes'][0]])

            if 'condition' in failure:
                nodes_from = [system.node_by_name(n) for n in failure['nodes']]
                predicate = Predicate.construct(failure['condition'], nodes_from, flow, can_inspect_results=False)
            else:
                predicate = Predicate.construct_default(flow)

            cls._add_failure_info(failure_node, failure, predicate)
            predicates.append(predicate)

        # we could make enumerable and avoid last_allocated (it would be cleaner), but let's stick with
        # this one for now
        return last_allocated, starting_failures, predicates
