#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Task and flow failure handling."""

from itertools import chain

from .errors import ConfigurationError
from .failure_node import FailureNode
from .helpers import check_conf_keys


class Failures:
    """Node failures and fallback handling."""

    def __init__(self, raw_definition, system, flow, last_allocated=None, starting_nodes=None, predicates=None):
        """Construct failures based on definition stated in YAML config files.

        :param raw_definition: raw definition of failures
        :param system: system context
        :param last_allocated: last allocated starting node for linked list
        :param starting_nodes: starting nodes for failures
        :param predicates: all predicates that are used
        """
        self.waiting_nodes = []
        self.fallback_nodes = []

        for failure in raw_definition:
            waiting_nodes_entry = []
            for node_name in failure['nodes']:
                node = system.node_by_name(node_name, graceful=True)
                if not node:
                    raise ConfigurationError("No such node with name '%s' in failure, flow '%s'"
                                             % (node_name, flow.name))
                waiting_nodes_entry.append(node)

            if isinstance(failure['fallback'], list):
                fallback_nodes_entry = []
                for node_name in failure['fallback']:
                    node = system.node_by_name(node_name, graceful=True)
                    if not node:
                        raise ConfigurationError("No such node with name '%s' in failure fallback, flow '%s'"
                                                 % (node_name, flow.name))
                    fallback_nodes_entry.append(node)
            elif isinstance(failure['fallback'], bool):
                fallback_nodes_entry = failure['fallback']
            else:
                raise ConfigurationError("Unknown fallback definition in flow '%s', failure: %s"
                                         % (flow.name, failure))

            self.waiting_nodes.append(waiting_nodes_entry)
            self.fallback_nodes.append(fallback_nodes_entry)

        self.raw_definition = raw_definition
        self.last_allocated = last_allocated
        self.starting_nodes = starting_nodes
        self.predicates = predicates
        self.flow = flow

    def all_waiting_nodes(self):
        """Compute all nodes that have defined a fallback.

        :return: all nodes that for which there is defined a callback
        """
        return list(set(chain(*self.waiting_nodes)))

    def all_fallback_nodes(self):
        """Compute all fallback nodes.

        :return: all nodes that are used as fallback nodes
        """
        # remove True/False flags
        nodes = []
        if isinstance(self.fallback_nodes, bool):
            return nodes

        for fallback in self.fallback_nodes:
            if isinstance(fallback, bool):
                continue

            for node in fallback:
                if not isinstance(node, bool):
                    nodes.append(node)

        return list(set(nodes))

    @staticmethod
    def construct(system, flow, failures_dict):
        """Construct Failres.

        :param system: system context
        :param flow: a flow to which failures conform
        :param failures_dict: construct failures from failures dict
        :rtype: Failures
        """
        for failure in failures_dict:
            if 'nodes' not in failure or failure['nodes'] is None:
                raise ConfigurationError("Failure should state nodes for state 'nodes' to fallback from in flow '%s'"
                                         % flow.name)

            if 'fallback' not in failure:
                raise ConfigurationError("No fallback stated in failure in flow '%s'" % flow.name)

            if not isinstance(failure['nodes'], list):
                failure['nodes'] = [failure['nodes']]

            if not isinstance(failure['fallback'], list) and failure['fallback'] is not True:
                failure['fallback'] = [failure['fallback']]

            if failure['fallback'] is not True and len(failure['fallback']) == 1 and len(failure['nodes']) == 1 \
                    and failure['fallback'][0] == failure['nodes'][0]:
                raise ConfigurationError("Detect cyclic fallback dependency in flow %s, failure on %s"
                                         % (flow.name, failure['nodes'][0]))

            known_conf_opts = ('nodes', 'fallback', 'propagate_failure', 'condition')
            unknown_conf = check_conf_keys(failure, known_conf_opts=known_conf_opts)
            if unknown_conf:
                raise ConfigurationError("Unknown configuration option supplied in fallback definition: %s"
                                         % unknown_conf)

        last_allocated, starting_nodes, predicates = FailureNode.construct(flow, system, failures_dict)
        return Failures(failures_dict, system, flow, last_allocated, starting_nodes, predicates)

    @staticmethod
    def starting_nodes_name(flow_name):
        """Create a starting node name for graph of all failures nodes for generated Python config.

        :param flow_name: flow name for which the starting node should be created
        :return: variable name
        :rtype: str
        """
        return "_%s_failure_starting_nodes" % flow_name

    @staticmethod
    def failure_node_name(flow_name, failure_node):
        """Create a failure node name representation for generated Python config.

        :param flow_name: name of flow for which the representation should be created
        :param failure_node: a node from graph of all failure permutations
        :return: variable name
        :rtype: str
        """
        return "_%s_fail_%s" % (flow_name, "_".join(failure_node.traversed))

    def fallback_nodes_names(self):
        """Compute names for all nodes that are started by fallbacks.

        :return: names of nodes that are started by fallbacks in all failures
        """
        ret = []

        failure_node = self.last_allocated
        while failure_node:
            if isinstance(failure_node.fallback, list):
                ret.extend(failure_node.fallback)
            failure_node = failure_node.failure_link

        return ret

    def waiting_nodes_names(self):
        """Compute all nodes that have defined fallbacks.

        :return: names of all nodes that we are expecting to fail for fallbacks
        """
        return list(self.starting_nodes.keys())

    def dump_all_conditions2stream(self, stream):
        """Dump all condition sources that are present to a stream.

        :param stream: output stream to dump to
        """
        fail_node = self.last_allocated

        while fail_node:
            for idx, predicate in enumerate(fail_node.predicates):
                condition_name = FailureNode.construct_condition_name(fail_node, idx)
                condition_source = predicate.to_source()

                stream.write('def {}(db, node_args):\n'.format(condition_name))
                stream.write('    return {}\n\n\n'.format(condition_source))

            fail_node = fail_node.failure_link

    def dump2stream(self, stream):
        """Dump failures to the Python config file for Dispatcher.

        :param stream: output stream to dump to
        """
        fail_node = self.last_allocated

        while fail_node:
            next_dict = {}
            for key, value in fail_node.next.items():
                next_dict[key] = self.failure_node_name(self.flow.name, value)

            stream.write("%s = {'next': " % self.failure_node_name(self.flow.name, fail_node))

            # print "next_dict"
            stream.write('{')
            printed = False
            for key, value in next_dict.items():
                if printed:
                    stream.write(", ")
                stream.write("'%s': %s" % (key, value))
                printed = True
            stream.write('}, ')

            conditions = []
            condition_strs = []
            for idx, predicate in enumerate(fail_node.predicates):
                conditions.append(FailureNode.construct_condition_name(fail_node, idx))
                condition_strs.append(str(predicate).replace('\'', '\\\''))

            # now list of nodes that should be started in case of failure (fallback)
            stream.write("'fallback': %s, 'propagate_failure': %s, 'conditions': %s, 'condition_strs': %s}\n"
                         % (fail_node.fallbacks, fail_node.propagate_failures,
                            str(conditions).replace("'", ""), condition_strs))
            fail_node = fail_node.failure_link

        stream.write("\n%s = {" % self.starting_nodes_name(self.flow.name))

        printed = False
        for key, value in self.starting_nodes.items():
            if printed:
                stream.write(",")
            stream.write("\n    '%s': %s" % (key, self.failure_node_name(self.flow.name, value)))
            printed = True
        stream.write("\n}\n\n")
