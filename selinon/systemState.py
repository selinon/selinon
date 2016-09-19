#!/bin/env python3
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
import json
from functools import reduce
from celery.result import AsyncResult
from .flowError import FlowError
from .storagePool import StoragePool
from .trace import Trace
from .config import Config
from .selinonTaskEnvelope import SelinonTaskEnvelope


class SystemState(object):
    """
    Main system actions done by Selinon
    """

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

    def __init__(self, dispatcher_id, flow_name, node_args = None, retry = None, state = None, parent=None,
                 finished=None):
        state_dict = {} if state is None else state

        self._dispatcher_id = dispatcher_id
        self._flow_name = flow_name
        self._node_args = node_args
        self._parent = parent if parent else {}
        self._finished = finished
        self._active_nodes = self._instantiate_active_nodes(state_dict.get('active_nodes', []))
        self._finished_nodes = state_dict.get('finished_nodes', {})
        self._failed_nodes = state_dict.get('failed_nodes', {})
        self._waiting_edges_idx = state_dict.get('waiting_edges', [])
        # Instantiate lazily later if we will know that there is something to process
        self._waiting_edges = []
        self._retry = retry

    def to_dict(self):
        """
        :return: converted system state to dict
        """
        return {'active_nodes': self._deinstantiate_active_nodes(self._active_nodes),
                'finished_nodes': self._finished_nodes,
                'failed_nodes': self._failed_nodes,
                'waiting_edges': self._waiting_edges_idx
                }

    def _get_successful_and_failed(self):
        """
        :return: all successful and failed nodes in system from active nodes
        """
        ret_successful = []
        ret_failed = []

        new_active_nodes = []
        for node in self._active_nodes:
            if node['result'].successful():
                Trace.log(Trace.NODE_SUCCESSFUL, {'flow_name': self._flow_name,
                                                  'dispatcher_id': self._dispatcher_id,
                                                  'node_name': node['name'],
                                                  'node_id': node['id']})
                ret_successful.append(node)
            elif node['result'].failed():
                Trace.log(Trace.NODE_FAILURE, {'flow_name': self._flow_name,
                                               'dispatcher_id': self._dispatcher_id,
                                               'node_name': node['name'],
                                               'node_id': node['id'],
                                               'what': node['result'].result})
                # We keep track of failed nodes to handle failures once all nodes finish
                if node['name'] not in self._failed_nodes:
                    self._failed_nodes[node['name']] = []
                self._failed_nodes[node['name']].append(node['id'])
                ret_failed.append(node['id'])
            else:
                new_active_nodes.append(node)

        self._active_nodes = new_active_nodes

        return ret_successful, ret_failed

    def _start_node(self, node_name, parent, node_args, finished=None):
        """
        Start a node in the system

        :param node_name: name of a node to be started
        :param parent: parent nodes of the starting node
        :param node_args: arguments for the starting node
        :param finished: finished nodes for starting node in case of subflow and propagate_finished flag
        """
        from .dispatcher import Dispatcher
        if Config.is_flow(node_name):
            if Config.propagate_node_args.get(self._flow_name):
                if Config.propagate_node_args.get(self._flow_name) is True or \
                    (isinstance(Config.propagate_node_args.get(self._flow_name), list) and
                     node_name in Config.propagate_node_args.get(self._flow_name)):
                    node_args = node_args
                else:
                    node_args = None
            else:
                node_args = None

            if Config.propagate_parent.get(self._flow_name):
                if Config.propagate_parent.get(self._flow_name) is True or \
                    (isinstance(Config.propagate_parent.get(self._flow_name), list) and
                     node_name in Config.propagate_parent.get(self._flow_name)):
                    parent = parent
                else:
                    parent = None
            else:
                parent = None

            kwargs = {
                'flow_name': node_name,
                'node_args': node_args,
                'parent': parent,
                'finished': finished
            }

            async_result = Dispatcher().apply_async(kwargs=kwargs, queue=Config.dispatcher_queue)

            # reuse kwargs for trace log entry
            kwargs['flow_name'] = self._flow_name
            kwargs['child_flow_name'] = node_name
            kwargs['dispatcher_id'] = self._dispatcher_id
            kwargs['child_dispatcher_id'] = async_result.task_id,
            kwargs['queue'] = Config.dispatcher_queue
            Trace.log(Trace.SUBFLOW_SCHEDULE, kwargs)
        else:
            kwargs = {
                'task_name': node_name,
                'flow_name': self._flow_name,
                'parent': parent,
                'node_args': node_args,
                'finished': finished
            }

            async_result = SelinonTaskEnvelope().apply_async(kwargs=kwargs, queue=Config.task_queues[node_name])

            # reuse kwargs for trace log entry
            kwargs['dispatcher_id'] = self._dispatcher_id
            kwargs['task_id'] = async_result.task_id
            Trace.log(Trace.TASK_SCHEDULE, kwargs)

        record = {'name': node_name, 'id': async_result.task_id, 'result': async_result}

        if node_name not in Config.nowait_nodes.get(self._flow_name, {}):
            self._active_nodes.append(record)

        return record

    def _run_fallback(self):
        """
        Run fallback in the system

        :return: fallbacks that were run
        """
        # we sort it first to make evaluation dependent on alphabetical order
        # TODO: use binary search when inserting to optimize from O(N*log(N)) to O(log(N))
        ret = []
        failed_nodes = sorted(self._failed_nodes.items())

        for i in range(len(failed_nodes), 0, -1):
            for combination in itertools.combinations(failed_nodes, i):
                try:
                    failure_nodes = Config.failures[self._flow_name]
                    failure_node = reduce(lambda n, c: n['next'][c[0]], combination[1:], failure_nodes[combination[0][0]])
                except KeyError:
                    # such failure not found in the tree of permutations - this means that this flow will always fail,
                    # but run defined fallbacks for defined failures first
                    continue

                if isinstance(failure_node['fallback'], list) and len(failure_node['fallback']) > 0:
                    parent = {}
                    finished = {}
                    traced_nodes_arr = []

                    for node in combination:
                        traced_nodes_arr.append((node[0], self._failed_nodes[node[0]],))

                        if Config.is_flow(node[0]) and Config.propagate_finished:
                            # we have to add all subsequent finished nodes to list to track info about finished
                            task_id = self._failed_nodes[node[0]][0]
                            # Celery stores exception that was raised in case of failure in result property
                            # We keep there information about finished and failed nodes
                            raw_result = str(AsyncResult(task_id).result)
                            flow_info = json.loads(raw_result)
                            parent[node[0]] = {}
                            finished[node[0]] = {}
                            self._extend_parent_finished(parent[node[0]], finished[node[0]], flow_info)
                            self._failed_nodes[node[0]].pop(0)
                        else:
                            parent[node[0]] = self._failed_nodes[node[0]].pop(0)

                        if len(self._failed_nodes[node[0]]) == 0:
                            del self._failed_nodes[node[0]]

                    Trace.log(Trace.FALLBACK_START, {'flow_name': self._flow_name,
                                                     'dispatcher_id': self._dispatcher_id,
                                                     'nodes': traced_nodes_arr,
                                                     'fallback': failure_node['fallback']})

                    for node in failure_node['fallback']:
                        record = self._start_node(node, parent=parent, node_args=self._node_args,
                                                  finished=finished)
                        ret.append(record)

                    # wait for fallback to finish in order to avoid time dependent flow evaluation
                    return ret
                elif failure_node['fallback'] is True:
                    traced_nodes_arr = []
                    for node in combination:
                        traced_nodes_arr.append((node[0], self._failed_nodes[node[0]],))
                        self._failed_nodes[node[0]].pop(0)
                        if len(self._failed_nodes[node[0]]) == 0:
                            del self._failed_nodes[node[0]]

                    Trace.log(Trace.FALLBACK_START, {'flow_name': self._flow_name,
                                                     'dispatcher_id': self._dispatcher_id,
                                                     'nodes': traced_nodes_arr,
                                                     'fallback': True})
                    # continue with fallback in other combinations, nothing started

        return ret

    def _update_waiting_edges(self, node_name):
        """
        Update waiting edges (edges that wait for a node finish) based on node name

        :param node_name: node that will trigger an edge
        """
        for idx, edge in enumerate(Config.edge_table[self._flow_name]):
            if node_name in edge['from'] and idx not in self._waiting_edges_idx:
                self._waiting_edges.append(edge)
                self._waiting_edges_idx.append(idx)

    def _extend_parent_finished(self, parent_dict, finished_dict, flow_info):
        """"
        Compute finished and parent for starting node in case of propagate_finished flag

        :param parent_dict: parent dict that should be extended
        :param finished_dict: finished dict that should be extended
        :param flow_info: flow info propagated from a flow
        """
        for node_name, node_val in flow_info['finished_nodes'].items():
            if Config.is_flow(node_name):
                finished_dict[node_name] = {}
                if node_name not in parent_dict:
                    parent_dict[node_name] = {}
                for node_id in node_val:
                    raw_result = str(AsyncResult(node_id).result)
                    child_flow_info = json.loads(raw_result)
                    self._extend_parent_finished(parent_dict[node_name], finished_dict[node_name], child_flow_info)
            else:
                finished_dict[node_name] = node_val

        for node_name, node_val in flow_info['failed_nodes'].items():
            if Config.is_flow(node_name):
                parent_dict[node_name] = {}
                if node_name not in finished_dict:
                    finished_dict[node_name] = {}
                for node_id in node_val:
                    result = AsyncResult(node_id).result

                    if not isinstance(result, FlowError):
                        # If we had some exception that was caused not by error in flow (e.g. bug in dispatcher, failed
                        # to connect to db, ...) we propagate it
                        raise result

                    child_flow_info = json.loads(str(result))
                    self._extend_parent_finished(parent_dict[node_name], finished_dict[node_name], child_flow_info)
            else:
                parent_dict[node_name] = node_val

    def _extend_parent_from_flow(self, parent_dict, flow_id):
        """
        Compute parent in a flow in case of propagate_parent flag

        :param parent_dict: parent dict that should be extended
        :param flow_id: flow id that should be inspected and used to extend parent_dict
        """
        async_result = AsyncResult(flow_id)

        for node_name, node_ids in async_result.result['finished_nodes'].items():
            if Config.is_flow(node_name):
                parent_dict[node_name] = {}
                for node_id in node_ids:
                    self._extend_parent_from_flow(parent_dict[node_name], node_id)
            else:
                if node_name not in parent_dict:
                    parent_dict[node_name] = []
                for node_id in node_ids:
                    parent_dict[node_name].append(node_id)

    def _start_new_from_finished(self, new_finished):
        """
        Start new based on finished nodes

        :param new_finished: finished nodes based on which we should start new nodes
        :return: newly started nodes
        """
        ret = []

        if not self._node_args and len(new_finished) == 1 and len(self._active_nodes) == 0 and \
                len(self._finished_nodes) == 0:
            # propagate arguments from newly finished node only if:
            #  1. we did not define node arguments in original Dispatcher() call
            #  2. the flow started only with a one single node that just finished
            #  3. node was not a subflow
            # TODO: make this configurable from YAML
            if not Config.is_flow(new_finished[0]['name']):
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
                    storage_id_mapping = {}

                    for start_node in start_nodes:
                        if Config.is_flow(start_node['name']):
                            propagate_finished = Config.propagate_finished.get(self._flow_name, False)
                            if propagate_finished is True or (isinstance(propagate_finished, list) and
                                                              start_node['name'] in propagate_finished):
                                parent[start_node['name']] = {}
                                self._extend_parent_from_flow(parent[start_node['name']], start_node['id'])
                        else:
                            parent[start_node['name']] = start_node['id']
                            storage_id_mapping[start_node['name']] = start_node['id']

                    # We could also examine results of subflow, there could be passed a list of subflows with
                    # finished_nodes to 'condition' in order to do inspection
                    storage_pool = StoragePool(storage_id_mapping)
                    if edge['condition'](storage_pool, self._node_args):
                        for node_name in edge['to']:
                            record = self._start_node(node_name, parent=parent, node_args=self._node_args,
                                                      finished=self._finished)
                            ret.append(record)

            node_name = node['name']
            if not self._finished_nodes.get(node_name):
                self._finished_nodes[node_name] = []
            self._finished_nodes[node_name].append(node['id'])

        return ret

    def _start_and_update_retry(self):
        """
        Start the flow and update retry

        :return: new/next retry
        """
        new_started_nodes = []

        Trace.log(Trace.FLOW_START, {'flow_name': self._flow_name,
                                     'dispatcher_id': self._dispatcher_id,
                                     'args': self._node_args})
        start_edges = [edge for edge in Config.edge_table[self._flow_name] if len(edge['from']) == 0]
        if len(start_edges) == 0:
            # This should not occur since selinonlib raises exception if such occurs, but just to be sure...
            raise ValueError("No starting node found for flow '%s'!" % self._flow_name)

        for start_edge in start_edges:
            storage_pool = StoragePool()
            if start_edge['condition'](storage_pool, self._node_args):
                for node_name in start_edge['to']:
                    node = self._start_node(node_name, node_args=self._node_args, parent=self._parent,
                                            finished=self._finished)
                    if node_name not in Config.nowait_nodes.get(self._flow_name, {}):
                        self._update_waiting_edges(node_name)
                        new_started_nodes.append(node)

        self._retry = Config.strategy_function(previous_retry=None,
                                               active_nodes=self._active_nodes,
                                               failed_nodes=self._failed_nodes,
                                               new_started_nodes=new_started_nodes,
                                               new_fallback_nodes=[])

    def update(self):
        """
        Check the current state in the system and start new nodes if possible

        :return: retry count - can be None (do not retry dispatcher) or time in seconds to retry
        """

        if not self._active_nodes and not self._finished_nodes and not self._waiting_edges and not self._failed_nodes:
            # we are starting up
            self._start_and_update_retry()
        else:
            new_finished, new_failed = self._get_successful_and_failed()

            if new_finished or new_failed:
                # Instantiate results lazily
                self._waiting_edges = self._idxs2items(Config.edge_table[self._flow_name], self._waiting_edges_idx)

                for node in new_finished:
                    self._update_waiting_edges(node['name'])

                started = self._start_new_from_finished(new_finished)

                fallback_started = []
                # We wait until all active nodes finish if there are some failed nodes try to recover from failure,
                # otherwise mark flow as failed
                if len(self._active_nodes) == 0 and self._failed_nodes:
                    fallback_started = self._run_fallback()
                    if len(fallback_started) == 0 and len(self._failed_nodes) > 0:
                        # We will propagate state information so we can correctly propagate parent and failed nodes in
                        # parent flow if there is any
                        # We use JSON in order to use result backend with JSON configured so objects are not pickled
                        state_dict = self.to_dict()
                        state_info = {
                            'finished_nodes': state_dict['finished_nodes'],
                            'failed_nodes': state_dict['failed_nodes']
                        }
                        raise FlowError(json.dumps(state_info))

                self._retry = Config.strategy_function(previous_retry=self._retry,
                                                       active_nodes=self._active_nodes,
                                                       failed_nodes=self._failed_nodes,
                                                       new_started_nodes=started,
                                                       new_fallback_nodes=fallback_started)
            else:
                self._retry = Config.strategy_function(previous_retry=self._retry,
                                                       active_nodes=self._active_nodes,
                                                       failed_nodes=self._failed_nodes,
                                                       new_started_nodes=[],
                                                       new_fallback_nodes=[])
        return self._retry
