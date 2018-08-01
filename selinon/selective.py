#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Path traversal manipulation."""

from collections import deque
import copy
from itertools import chain

from .config import Config
from .errors import SelectiveNoPathError


def _get_all_subflows_dict(flow_name):
    """Get all subflows for the given flow name, the result is stored in a dict.

    The resulting dict has keys that correspond to all transitive subflows for flow flow_name and values for a subflow
    is a list of flows that are directly parent for corresponding subflow (key).

    E.g.:
      flow1:
        subflows: flow2, flow3

      flow2:
        subflows: flow1, flow3, flow4

    The resulting dict for _get_all_subflows_dict('flow1') will be:
       {
         "flow2": ["flow1"],
         "flow3": ["flow1", "flow2"],
         "flow4": ["flow2"],
         "flow1": ["flow2"],
       }

    Read as: I can get directly to flow <key> from flows <values>.


    :param flow_name: a flow name for which all subflows should be computed
    :return: dict representing all subflows and direct paths for the given subflow
    """
    stack = deque()
    result = {}

    stack.append(flow_name)
    while stack:
        inspected_flow_name = stack.pop()
        for edge in Config.edge_table[inspected_flow_name]:
            for node_name in edge['to']:
                if Config.is_flow(node_name):
                    if node_name not in result:
                        result[node_name] = set()
                        stack.append(node_name)
                    result[node_name] |= {inspected_flow_name}

    for key, value in result.items():
        result[key] = list(value)

    return result


def _normalize_path(paths):
    """Normalize multiple graph traversals by edges into one that traverses all edges.

    In general we can get to a task by multiple paths. As we would like to ensure that all paths are traversed, we
    compound multiple traversals into a single one that traverses all necessary edges.

    :param paths: a list of paths that should be compound into a single traversal
    :return: a dict representing compound traversal
    """
    result = {}
    for entry in paths:
        for key, value in entry.items():
            if key not in result:
                result[key] = []
            result[key] = list(set(result[key]) | set(value))

    return result


def _compute_paths(flow_name, task_name):
    """Compute all paths in a flow to a node.

    :param flow_name: name of flow that should be traversed
    :param task_name: a name of node that should be visited
    :return: a list of paths that lead to the node that should be visited
    """
    result = []
    stack = deque()
    stack.append(({task_name}, set(), {}))

    while stack:
        to_expand, visited, expanded = stack.pop()

        for name in to_expand:
            for edge_idx, edge in enumerate(Config.edge_table[flow_name]):
                if name in edge['to'] and (edge_idx not in expanded.keys() or name not in expanded[edge_idx]):
                    new_expanded = copy.copy(expanded)
                    if edge_idx not in new_expanded:
                        new_expanded[edge_idx] = [name]
                    else:
                        new_expanded[edge_idx].append(name)

                    new_visited = visited | {name}
                    new_to_expand = to_expand | set(edge['from']) - {name}

                    if new_visited == new_to_expand:
                        result.append(new_expanded)
                    else:
                        stack.append((new_to_expand, new_visited, new_expanded))

    return _normalize_path(result)


def _raise_for_result_check(task_names, path):
    """Check that all nodes stated in task_names are present in path - i.e. they are visited during traversing.

    :param task_names: a list of node names that should be checked
    :param path: traverses that are per-flow specific
    :raises SelectiveNoPathError: there was not found any path to the given node
    """
    visited_marking = dict.fromkeys(task_names, False)
    for node in visited_marking:
        for flow in path:
            if any(n == node for n in chain(*path[flow].values())):
                visited_marking[node] = True

    for node, visited in visited_marking.items():
        if not visited:
            raise SelectiveNoPathError("No path to node '%s' found" % node)


def _compute_subsequent_edges(flow_name, node_names):
    """Compute nodes that are subsequent nodes based on node_names.

    :param flow_name: name of the flow in which subsequent nodes should be found
    :param node_names: a list of nodes that were run, note that they does not need to be necessarily stated in flow_name
    :return: a list of tasks that follow after node_names execution
    """
    result = []
    desired_nodes = set(node_names)

    change = True
    while change:
        change = False
        for edge_idx, edge in enumerate(Config.edge_table[flow_name]):
            if edge['from'] and edge_idx not in result and set(edge['from']).issubset(desired_nodes):
                result.append(edge_idx)
                desired_nodes |= set(edge['to'])
                change = True

    return result


def _compute_traversals(flow_name, task_names, follow_subflows=True):
    """Compute all traversals/paths to nodes from a flow.

    :param flow_name: a name of flow to start traversing with
    :param task_names: a list of nodes we want to visit/traverse
    :param follow_subflows: if True, we also inspect transitively all subflows from flow_name
    :return: a dict, where keys are flows that need to be traversed and values are traversals/paths to all task_names
    """
    result = {}
    stack = deque()
    subflows_dict = {}
    traversed_subflows = set()

    if not isinstance(task_names, (list, tuple)):
        task_names = [task_names]

    for task_name in task_names:
        stack.append((flow_name, task_name))

    if follow_subflows:
        subflows_dict = _get_all_subflows_dict(flow_name)
        for subflow_name in subflows_dict.keys():  # pylint: disable=consider-iterating-dictionary
            for task_name in task_names:
                stack.append((subflow_name, task_name))

    while stack:
        flow, node = stack.pop()
        paths = _compute_paths(flow, node)

        if flow != flow_name and paths:
            for parent_flow in subflows_dict[flow]:
                if (parent_flow, flow) not in traversed_subflows:
                    traversed_subflows |= {(parent_flow, flow)}
                    stack.append((parent_flow, flow))

        if flow in result:
            result[flow] = _normalize_path((result[flow], paths))
        else:
            result[flow] = paths

    _raise_for_result_check(task_names, result)
    return result


def compute_selective_run(flow_name, task_names, follow_subflows=False, run_subsequent=False):
    """Compute selective run for a flow.

    :param flow_name: a name of the flow that should be run
    :param task_names: a list of tasks that should be run
    :param follow_subflows: apply selective run to all subflows (transitively)
    :param run_subsequent: run tasks that depend on task_names
    :return: computed selective run dictionary
    """
    traversals = _compute_traversals(flow_name, task_names, follow_subflows)
    result = {
        'task_names': task_names,
        'waiting_edges_subset': traversals
    }

    if run_subsequent:
        if isinstance(run_subsequent, (list, tuple)):
            subsequent_flows = run_subsequent
        else:
            subsequent_flows = traversals.keys()

        for flow in subsequent_flows:
            subsequent_edges = _compute_subsequent_edges(flow, task_names)
            for idx in subsequent_edges:
                # We need to make sure that we start all nodes so we have fire edge when we visit it twice due to cycles
                #
                #      T1 <-
                #    / |    |
                #   /  |    |
                # T3    T2 --
                #
                # T2 should be run, if we get minimal path to T2, there will be no T3, but since we have edge T2->T1, we
                # need to start T3 as subsequent.
                #
                traversals[flow][idx] = Config.edge_table[flow][idx]['to']

    return result
