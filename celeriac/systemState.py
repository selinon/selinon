#!/bin/env python
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import itertools
from celery.result import AsyncResult, states
from .flowError import FlowError
from .dispatcher import Dispatcher
from .storagePool import StoragePool


# TODO: write docstrings
class SystemState(object):
    # initial retry countdown
    _start_retry = 2
    # do not pass this retry countdown
    _max_retry = 120

    @property
    def node_args(self):
        """
        :return: arguments for a node
        """
        return self._node_args

    @staticmethod
    def _instantiate_active_nodes(arr):
        """
        :return: convert node references from argument to AsyncResult
        """
        return [{'name': node['name'], 'id': node['id'], 'result': AsyncResult(id=node['id'])} for node in arr]

    @staticmethod
    def _deinstantiate_active_nodes(arr):
        """
        :return: node references for Dispatcher arguments
        """
        return [{'name': x['name'], 'id': x['id']} for x in arr]

    @staticmethod
    def _idxs2items(edge_node_table, arr):
        # we keep only the index to edges_table in order to avoid serialization and optimize number representation
        ret = []
        for i in arr:
            ret.append(edge_node_table[i])
        return ret

    def __init__(self, edge_table, flow_name, node_args = None, retry = None, state = None):
        state_dict = {} if state is None else state

        self._get_task_instance = None
        self._is_flow = None

        self._edge_table = edge_table
        self._flow_name = flow_name
        self._node_args = node_args
        self._active_nodes = self._instantiate_active_nodes(state_dict.get('active_nodes', []))
        self._finished_nodes = state_dict.get('finished_nodes', {})
        self._waiting_edges_idx = state_dict.get('waiting_edges', [])
        self._waiting_edges = self._idxs2items(edge_table[flow_name], self._waiting_edges_idx)
        self._retry = retry if retry else self._start_retry

    def to_dict(self):
        return {'active_nodes': self._deinstantiate_active_nodes(self._active_nodes),
                'finished_nodes': self._finished_nodes,
                'waiting_edges': self._waiting_edges_idx
                }

    def _get_successful(self):
        ret = []

        new_active_nodes = []
        failure_nodes = []
        for node in self._active_nodes:
            if node['result'].successful():
                ret.append(node)
            elif node['result'].state == states.FAILURE:
                # TODO: Implement fallback
                failure_nodes.append(node)
            else:
                new_active_nodes.append(node)

        # We wait until all active nodes finish and then we raise an exception since we want to keep dispatcher alive
        # until there is anything active
        if len(new_active_nodes) == 0 and len(failure_nodes) > 0:
            raise FlowError("Nodes '%s' failed" % [(node['name'], node['id']) for node in failure_nodes])

        # keep failure nodes for the next iteration in case we still have something to do
        self._active_nodes = new_active_nodes + failure_nodes

        return ret

    def _start_node(self, node_name, parent, args):
        if self._is_flow(node_name):
            # TODO: should we pass id of parent?
            async_result = Dispatcher().delay(flow_name=node_name, args=None, parent=self._flow_name)
        else:
            task = self._get_task_instance(node_name)
            async_result = task.delay(task_name=node_name, flow_name=self._flow_name, parent=parent, args=args)

        return async_result

    def _update_waiting_edges(self, node_name):
        for idx, edge in enumerate(self._edge_table[self._flow_name]):
            if node_name in edge['from'] and idx not in self._waiting_edges_idx:
                self._waiting_edges.append(edge)
                self._waiting_edges_idx.append(idx)

    def _start_new_from_finished(self, new_finished):
        ret = []

        if not self._node_args and len(new_finished) == 1 and len(self._active_nodes) == 0 and \
                len(self._finished_nodes) == 0:
            # propagate arguments from newly finished node only if:
            #  1. we did not define node arguments in original Dispatcher() call
            #  2. the flow started only with a one single node that just finished
            self._node_args = new_finished[0]['result'].result

        for node in new_finished:
            # We could optimize this by pre-computing affected edges in pre-generated config file for each
            # node and computing intersection with waiting edges, but let's stick with this solution for now
            edges = [edge for edge in self._waiting_edges if node['name'] in edge['from']]

            for edge in edges:
                from_nodes = dict.fromkeys(edge['from'], [])

                for from_name in from_nodes:
                    if from_name == node['name']:
                        from_nodes[from_name] = [{'name': node['name'], 'id': node['id']}]
                    else:
                        from_nodes[from_name] = [{'name': from_name, 'id': n}
                                                 for n in self._finished_nodes.get(from_name, [])]

                # if there are multiple nodes of a same type waiting on an edge:
                #
                #   A    B
                #    \  /
                #     \/
                #     C
                #
                # A: <id1>, <id2>
                # B: <id3>, <id4>
                #
                # Compute all combinations: (<id1>, <id3>), (<id1>, <id4>), (<id2>, <id3>), (<id2>, <id4>)
                #
                for start_nodes in itertools.product(*from_nodes.values()):
                    parent = {}

                    # TODO: not list only ids
                    for start_node in start_nodes:
                        if not parent.get(start_node['name'], False):
                            parent[start_node['name']] = []
                        parent[start_node['name']].append(start_node['id'])

                    storage_pool = StoragePool(parent)
                    if edge['condition'](storage_pool):
                        for node_name in edge['to']:
                            ar = self._start_node(node_name, parent, self._node_args)
                            record = {'name': node_name, 'id': ar.task_id, 'result': ar}
                            self._active_nodes.append(record)
                            ret.append(record)

            node_name = node['name']
            if not self._finished_nodes.get(node_name):
                self._finished_nodes[node_name] = []
            self._finished_nodes[node_name].append(node['id'])

        return ret

    def _continue_and_update_retry(self, new_finished):
        started = self._start_new_from_finished(new_finished)
        if len(started) > 0:
            self._retry = self._start_retry
        elif len(self._active_nodes) > 0:
            self._retry = min(self._retry*2, self._max_retry)
        else:
            # nothing to process in the next run
            self._retry = None
        return self._retry

    def _start_and_update_retry(self):
        start_edges = [edge for edge in self._edge_table[self._flow_name] if len(edge['from']) == 0]
        if len(start_edges) == 0:
            # This should not occur since parsley raises exception if such occurs, but just to be sure...
            raise ValueError("No starting node found for flow '%s'!" % self._flow_name)

        for start_edge in start_edges:
            storage_pool = StoragePool()
            if start_edge['condition'](storage_pool):
                for node_name in start_edge['to']:
                    ar = self._start_node(node_name, args=self._node_args, parent=None)
                    self._active_nodes.append({'id': ar.task_id, 'name': node_name, 'result': ar})
                    self._update_waiting_edges(node_name)

        if len(self._active_nodes) > 0:
            self._retry = self._start_retry
        else:
            self._retry = None

        return self._retry

    def update(self, get_task_instance, is_flow):
        self._get_task_instance = get_task_instance
        self._is_flow = is_flow

        if not self._active_nodes and not self._finished_nodes and not self._waiting_edges:
            # we are starting up
            return self._start_and_update_retry()
        else:
            new_finished = self._get_successful()

            for node in new_finished:
                self._update_waiting_edges(node['name'])

            return self._continue_and_update_retry(new_finished)

