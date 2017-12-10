#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Edge representation in task/flow dependency graph."""

from .errors import ConfigurationError
from .helpers import check_conf_keys
from .predicate import Predicate


class Edge(object):
    """Edge representation."""

    _DEFAULT_RUN_SUBSEQUENT = False
    _DEFAULT_FOLLOW_SUBFLOWS = False

    def __init__(self, nodes_from, nodes_to, predicate, flow, foreach, selective):
        """Initialize edge definition.

        :param nodes_from: nodes from where edge starts
        :type nodes_from: List[Node]
        :param nodes_to: nodes where edge ends
        :type nodes_to: List[Node]
        :param predicate: predicate condition
        :type predicate: Predicate
        :param flow: flow to which edge belongs to
        :type flow: Flow
        :param foreach: foreach defining function and import over which we want to iterate
        :type foreach: dict
        :param selective: selective run flow configuration
        :type selective: None|dict
        """
        self.nodes_from = nodes_from
        self.nodes_to = nodes_to
        self.predicate = predicate
        self.flow = flow
        self.foreach = foreach
        self.selective = selective

    def check(self):
        """Check edge consistency."""
        if self.foreach and self.foreach['propagate_result']:
            # We can propagate result of our foreach function only if:
            #  1. all nodes to are flows
            #  2. propagate_node_args is not set for flows listed in nodes to
            for node_to in self.nodes_to:
                if not node_to.is_flow():
                    raise ConfigurationError("Flag propagate_result listed in foreach configuration in flow '%s' "
                                             "requires all nodes to to be flows, but '%s' is a task"
                                             % (self.flow.name, node_to.name))

                if (isinstance(self.flow.propagate_node_args, bool) and self.flow.propagate_node_args) \
                        or \
                    (isinstance(self.flow.propagate_node_args, list) and node_to.name in self.flow.propagate_node_args):
                    raise ConfigurationError("Cannot propagate node arguments to subflow when propagate_result "
                                             "is set in foreach definition in flow '%s' for node to '%s'"
                                             % (self.flow.name, node_to.name))

        for node in self.predicate.nodes_used():
            if not node.is_task():
                continue

            if node.storage_readonly and self.predicate.requires_message():
                raise ConfigurationError("Cannot inspect results of node '%s' in flow '%s' as this node is "
                                         "configured with readonly storage, condition: %s"
                                         % (node.name, self.flow.name, str(self.predicate)))

        if self.selective:
            # TODO: there are no checks whether tasks that we want to run are actually run - this will be runtime error
            if len(self.nodes_to) > 1:
                raise ConfigurationError("Cannot run selective flows on edges with more than 1 destination nodes in"
                                         "flow '%s'" % self.flow.name)

            if not self.nodes_to[0].is_flow():
                raise ConfigurationError("Cannot run selective flow in flow '%s', destination node '%s' is not a flow,"
                                         % (self.flow.name, self.nodes_to[0].name))

    def foreach_str(self):
        """Construct string representation for foreach node.

        :return: text representation of foreach
        """
        if self.foreach:
            return "foreach %s.%s" % (self.foreach['import'], self.foreach['function'])

        return None

    @classmethod
    def _parse_selective(cls, flow, selective_def):
        """Parse selective edge run definition.

        :param flow: flow in which selective subflow should be run
        :param selective_def: selective flow definition
        :return: parsed selective_flow
        """
        if not selective_def:
            return None

        unknown_conf = check_conf_keys(selective_def, known_conf_opts=('tasks', 'run_subsequent', 'follow_subflows'))
        if unknown_conf:
            raise ConfigurationError("Unknown configuration options supplied for selective flow run in flow '%s': %s"
                                     % (flow.name, unknown_conf))

        if 'tasks' not in selective_def:
            raise ConfigurationError("Configuration for selective subflows expects 'tasks' to be defined in flow '%s'"
                                     % flow.name)

        if not isinstance(selective_def['tasks'], list) or not all(isinstance(t, str) for t in selective_def['tasks']):
            raise ConfigurationError("Configuration for selective subflows expects task names under 'tasks' key in "
                                     "flow '%s', got '%s' instead" % (flow.name, selective_def['tasks']))

        if 'run_subsequent' not in selective_def:
            selective_def['run_subsequent'] = cls._DEFAULT_RUN_SUBSEQUENT

        if 'follow_subflows' not in selective_def:
            selective_def['follow_subflows'] = cls._DEFAULT_FOLLOW_SUBFLOWS

        if not isinstance(selective_def['run_subsequent'], bool) and \
                not isinstance(selective_def['run_subsequent'], list):
            raise ConfigurationError("Option 'run_subsequent' requires boolean or list of flow names "
                                     "in which subsequent nodes should be run in flow '%s', got '%s'"
                                     % (flow.name, selective_def['run_subsequent']))

        if not isinstance(selective_def['follow_subflows'], bool):
            raise ConfigurationError("Option 'follow_subflows' expects boolean got '%s' instead in flow '%s'"
                                     % (selective_def['follow_subflows'], flow.name))

        # So we can use directly selective as **kwargs to run_flow_selective
        # TODO: unify with run_flow_selective
        selective_def['task_names'] = selective_def.pop('tasks')

        return selective_def

    @classmethod
    def from_dict(cls, dict_, system, flow):  # pylint: disable=too-many-branches
        """Construct edge from a dict.

        :param dict_: a dictionary from which the system should be created
        :type dict_: dict
        :param system:
        :type system: System
        :param flow: flow to which edge belongs to
        :type flow: Flow
        :return: edge representation
        :rtype: Edge
        """
        if 'from' not in dict_:
            raise ConfigurationError("Edge definition requires 'from' explicitly to be specified, "
                                     "use empty for starting edge")

        # we allow empty list for a starting edge
        if dict_['from']:
            from_names = dict_['from'] if isinstance(dict_['from'], list) else [dict_['from']]
            nodes_from = [system.node_by_name(n) for n in from_names]
        else:
            nodes_from = []

        if 'to' not in dict_ or not dict_['to']:
            raise ConfigurationError("Edge definition requires 'to' specified")

        to_names = dict_['to'] if isinstance(dict_['to'], list) else [dict_['to']]
        nodes_to = [system.node_by_name(n) for n in to_names]

        if 'condition' in dict_:
            predicate = Predicate.construct(dict_.get('condition'), nodes_from, flow)
        else:
            predicate = Predicate.construct_default(flow)

        foreach = None
        if 'foreach' in dict_:
            foreach_def = dict_['foreach']
            if foreach_def is None or 'function' not in foreach_def or 'import' not in foreach_def:
                raise ConfigurationError("Specification of 'foreach' requires 'function' and 'import' to be set "
                                         "in flow '%s', got %s instead" % (flow.name, foreach_def))

            foreach = {
                'function': foreach_def['function'],
                'import': foreach_def['import'],
                'propagate_result': False
            }

            if 'propagate_result' in foreach_def:
                if not isinstance(foreach_def['propagate_result'], bool):
                    raise ConfigurationError("Propagate result should be bool in flow '%s', got %s instead"
                                             % (flow.name, foreach_def['propagate_result']))

                if foreach_def['propagate_result']:
                    foreach['propagate_result'] = True

                # additional checks for 'propagate_result' are done in Edge.check() since we have chicken-egg problem
                # here

            if not isinstance(foreach_def['function'], str):
                raise ConfigurationError("Wrong function name '%s' supplied in foreach section in flow %s"
                                         % (foreach_def['function'], flow.name))

            if not isinstance(foreach_def['import'], str):
                raise ConfigurationError("Wrong import statement '%s' supplied in foreach section in flow %s"
                                         % (foreach_def['import'], flow.name))

        selective = cls._parse_selective(flow, dict_.get('selective'))

        unknown_conf = check_conf_keys(dict_, known_conf_opts=('from', 'to', 'foreach', 'condition', 'selective'))
        if unknown_conf:
            raise ConfigurationError("Unknown configuration options supplied for edge in flow '%s': %s"
                                     % (flow.name, unknown_conf))

        return Edge(
            nodes_from=nodes_from,
            nodes_to=nodes_to,
            predicate=predicate,
            flow=flow,
            foreach=foreach,
            selective=selective
        )
