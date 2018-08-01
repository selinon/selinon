#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Core Selinon logic for system representation, parsing and handling actions."""

import datetime
import logging
import os
import platform

import yaml

import graphviz

from .errors import ConfigurationError
from .flow import Flow
from .global_config import GlobalConfig
from .helpers import check_conf_keys
from .helpers import dict2strkwargs
from .helpers import expr2str
from .selective_run_function import SelectiveRunFunction
from .storage import Storage
from .task import Task
from .task_class import TaskClass
from .user_config import UserConfig
from .version import selinon_version

# pylint: disable=too-many-locals,too-many-nested-blocks,too-many-boolean-expressions,too-many-lines


class System:
    """The representation of the whole system."""

    _logger = logging.getLogger(__name__)

    def __init__(self, tasks=None, flows=None, storages=None, task_classes=None):
        """Initialize system info.

        :param tasks: a list of tasks available in the system
        :param flows: a list of flows available in the system
        :param storages: a list of storages available in the system
        :param task_classes: a list of classes that implement task logic (the same Python class can be represented
                             in multiple Selinon tasks)
        """
        self.flows = flows or []
        self.tasks = tasks or []
        self.storages = storages or []
        self.task_classes = task_classes or []

    def _check_name_collision(self, name):
        """All tasks and flows share name space, check for collisions.

        :param name: a node name
        :raises: ConfigurationError
        """
        if any(flow.name == name for flow in self.flows):
            raise ConfigurationError("Unable to add node with name '%s', a flow with the same name already exist"
                                     % name)
        if any(task.name == name for task in self.tasks):
            raise ConfigurationError("Unable to add node with name '%s', a task with the same name already exist"
                                     % name)

    def add_task(self, task):
        """Register a task in the system.

        :param task: a task to be registered
        :type task: selinon.selinon_task.Task
        """
        self._check_name_collision(task.name)
        self.tasks.append(task)

    def add_flow(self, flow):
        """Register a flow in the system.

        :param flow: a flow to be registered
        :type flow: flow
        """
        self._check_name_collision(flow.name)
        self.flows.append(flow)

    def add_storage(self, storage):
        """Add storage to system.

        :param storage: storage that should be added
        """
        # We need to check for name collision with tasks as well since we import them by name
        for task in self.tasks:
            if task.name == storage.name:
                raise ConfigurationError("Storage has same name as task '%s'" % storage.name)

        for stored_storage in self.storages:
            if stored_storage.name == storage:
                raise ConfigurationError("Multiple storages of the same name '{}'".format(storage.name))
        self.storages.append(storage)

    def storage_by_name(self, name, graceful=False):
        """Retrieve storage by its name.

        :param name: name of the storage
        :param graceful: if true, exception is raised if no such storage with name name is found
        :return: storage
        """
        for storage in self.storages:
            if storage.name == name:
                return storage
        if not graceful:
            raise ConfigurationError("Storage with name {} not found in the system".format(name))
        return None

    def task_by_name(self, name, graceful=False):
        """Find a task by its name.

        :param name: a task name
        :param graceful: if True, no exception is raised if task couldn't be found
        :return: task with name 'name'
        :rtype: selinon.selinon_task.Task
        :raises ConfigurationError: on non-existing task name
        """
        for task in self.tasks:
            if task.name == name:
                return task

        if not graceful:
            raise ConfigurationError("Task with name {} not found in the system".format(name))

        return None

    def flow_by_name(self, name, graceful=False):
        """Find a flow by its name.

        :param name: a flow name
        :param graceful: if True, no exception is raised if flow couldn't be found
        :return: flow with name 'name'
        :rtype: Flow
        :raises ConfigurationError: on non-existing flow name
        """
        for flow in self.flows:
            if flow.name == name:
                return flow

        if not graceful:
            raise ConfigurationError("Flow with name '{}' not found in the system".format(name))

        return None

    def node_by_name(self, name, graceful=False):
        """Find a node (flow or task) by its name.

        :param name: a node name
        :param graceful: if True, no exception is raised if node couldn't be found
        :return: flow with name 'name'
        :rtype: Node
        :raises: KeyError
        """
        node = self.task_by_name(name, graceful=True)

        if not node:
            node = self.flow_by_name(name, graceful=True)

        if not node and not graceful:
            raise ConfigurationError("Entity with name '{}' not found in the system".format(name))

        return node

    def task_queue_names(self):
        """Get information about queue names per task.

        :return: corresponding mapping from task name to task queue
        """
        ret = {}

        for task in self.tasks:
            ret[task.name] = task.queue_name

        return ret

    def dispatcher_queue_names(self):
        """Get information about queue names per dispatcher/flow.

        :return: dispatcher queue names based on flow names
        """
        ret = {}

        for flow in self.flows:
            ret[flow.name] = flow.queue_name

        return ret

    def class_of_task(self, task):
        """Return task class of a task.

        :param task: task to look task class for
        :return: TaskClass or None if a task class for task is not available
        """
        for task_class in self.task_classes:
            if task_class.task_of_class(task):
                return task_class
        return None

    def _dump_imports(self, output):
        """Dump used imports of tasks to a stream.

        :param output: a stream to write to
        """
        predicates = set()
        cache_imports = set()
        selective_run_function_imports = set()

        for flow in self.flows:
            for edge in flow.edges:
                predicates.update([p.__name__ for p in edge.predicate.predicates_used()])

            # predicates used on failure nodes
            if flow.failures:
                for predicate in flow.failures.predicates:
                    predicates.update([p.__name__ for p in predicate.predicates_used()])

            cache_imports.add((flow.cache_config.import_path, flow.cache_config.name))

        if predicates:
            output.write('from %s import %s\n' % (GlobalConfig.predicates_module, ", ".join(predicates)))

        for flow in self.flows:
            for idx, edge in enumerate(flow.edges):
                if edge.foreach:
                    output.write('from {} import {} as {}\n'.format(edge.foreach['import'],
                                                                    edge.foreach['function'],
                                                                    self._dump_foreach_function_name(flow.name, idx)))

        for task in self.tasks:
            output.write("from {} import {} as {}\n".format(task.import_path, task.class_name, task.name))
            f = task.selective_run_function  # pylint: disable=invalid-name
            selective_run_function_imports.add((f.import_path, f.name))

        for storage in self.storages:
            output.write("from {} import {}\n".format(storage.import_path, storage.class_name))
            cache_imports.add((flow.cache_config.import_path, flow.cache_config.name))

        for import_path, cache_name in cache_imports:
            output.write("from {} import {}\n".format(import_path, cache_name))

        for import_path, function_name in selective_run_function_imports:
            output.write("from {} import {} as {}\n".format(
                import_path,
                function_name,
                SelectiveRunFunction.construct_import_name(function_name, import_path)
            ))

        # we need partial for strategy function and for using storage as trace destination
        output.write("\nimport functools\n")
        # we need datetime for timedelta in throttling
        output.write("\nimport datetime\n")

    def _dump_output_schemas(self, output):
        """Dump output schema mapping to a stream.

        :param output: a stream to write to
        """
        output.write('output_schemas = {')
        printed = False
        for task in self.tasks:
            if task.output_schema:
                if printed:
                    output.write(",")
                output.write("\n    '%s': '%s'" % (task.name, task.output_schema))
                printed = True
        output.write('\n}\n\n')

    def _dump_flow_flags(self, stream):
        """Dump various flow flags.

        :param stream: a stream to write to
        """
        self._dump_dict(stream,
                        'node_args_from_first',
                        {f.name: f.node_args_from_first for f in self.flows})

        self._dump_dict(stream,
                        'propagate_node_args',
                        {f.name: f.propagate_node_args for f in self.flows})

        self._dump_dict(stream,
                        'propagate_parent',
                        {f.name: f.propagate_parent for f in self.flows})

        self._dump_dict(stream,
                        'propagate_parent_failures',
                        {f.name: f.propagate_parent_failures for f in self.flows})

        self._dump_dict(stream,
                        'propagate_finished',
                        {f.name: f.propagate_finished for f in self.flows})

        self._dump_dict(stream,
                        'propagate_compound_finished',
                        {f.name: f.propagate_compound_finished for f in self.flows})

        self._dump_dict(stream,
                        'propagate_failures',
                        {f.name: f.propagate_failures for f in self.flows})

        self._dump_dict(stream,
                        'propagate_compound_failures',
                        {f.name: f.propagate_compound_failures for f in self.flows})

    @staticmethod
    def _dump_dict(output, dict_name, dict_items):
        """Dump propagate_finished flag configuration to a stream.

        :param output: a stream to write to
        """
        output.write('%s = {' % dict_name)
        printed = False
        for key, value in dict_items.items():
            if printed:
                output.write(",")
            if isinstance(value, list):
                string = [item.name for item in value]
            else:
                string = str(value)
            output.write("\n    '%s': %s" % (key, string))
            printed = True
        output.write('\n}\n\n')

    def _dump_task_classes(self, output):
        """Dump mapping from task name to task class.

        :param output: a stream to write to
        """
        output.write('task_classes = {')
        printed = False
        for task in self.tasks:
            if printed:
                output.write(',')
            output.write("\n    '%s': %s" % (task.name, task.name))
            printed = True
        output.write('\n}\n\n')

    def _dump_storage_task_names(self, output):
        """Dump mapping for task name in storage (task name alias for storage).

        :param output: a stream to write to
        """
        output.write('storage_task_name = {')
        printed = False
        for task in self.tasks:
            if printed:
                output.write(',')
            output.write("\n    '%s': '%s'" % (task.name, task.storage_task_name))
            printed = True
        output.write('\n}\n\n')

    def _dump_queues(self, output):
        """Dump queues for tasks and dispatcher.

        :param output: a stream to write to
        """
        self._dump_dict(output, 'task_queues', {f.name: "'%s'" % f.queue_name for f in self.tasks})
        self._dump_dict(output, 'dispatcher_queues', {f.name: "'%s'" % f.queue_name for f in self.flows})

    def _dump_storage2instance_mapping(self, output):
        """Dump storage name to instance mapping to a stream.

        :param output: a stream to write to
        """
        storage_var_names = []
        for storage in self.storages:
            output.write("%s = %s" % (storage.var_name, storage.class_name))
            if storage.configuration and isinstance(storage.configuration, dict):
                output.write("(%s)\n" % dict2strkwargs(storage.configuration))
            elif storage.configuration:
                output.write("(%s)\n" % expr2str(storage.configuration))
            else:
                output.write("()\n")

            storage_var_names.append((storage.name, storage.var_name,))

        output.write('storage2instance_mapping = {\n')
        printed = False
        for storage_var_name in storage_var_names:
            if printed:
                output.write(",\n")
            output.write("    '%s': %s" % (storage_var_name[0], storage_var_name[1]))
            printed = True

        output.write("\n}\n\n")

    def _dump_task2storage_mapping(self, output):
        """Dump task name to storage name mapping to a stream.

        :param output: a stream to write to
        """
        output.write('task2storage_mapping = {\n')
        printed = False
        for task in self.tasks:
            if printed:
                output.write(",\n")
            storage_name = ("'%s'" % task.storage.name) if task.storage else str(None)
            output.write("    '%s': %s" % (task.name, storage_name))
            printed = True

        output.write("\n}\n\n")

    def _dump_storage_conf(self, output):
        """Dump storage configuration to a stream.

        :param output: a stream to write to
        """
        self._dump_task2storage_mapping(output)
        self._dump_storage2instance_mapping(output)
        for storage in self.storages:
            cache_config = storage.cache_config
            output.write("%s = %s(%s)\n" % (cache_config.var_name, cache_config.name,
                                            dict2strkwargs(cache_config.configuration)))
        self._dump_dict(output, 'storage2storage_cache', {s.name: s.cache_config.var_name for s in self.storages})

    def _dump_async_result_cache(self, output):
        """Dump Celery AsyncResult caching configuration.

        :param output: a stream to write to
        """
        for flow in self.flows:
            cache_config = flow.cache_config
            output.write("%s = %s(%s)\n" % (cache_config.var_name, cache_config.name,
                                            dict2strkwargs(cache_config.configuration)))
        self._dump_dict(output, 'async_result_cache', {f.name: f.cache_config.var_name for f in self.flows})

    def _dump_strategy_func(self, output):
        """Dump scheduling strategy function to a stream.

        :param output: a stream to write to
        """
        def strategy_func_name(flow):  # pylint: disable=missing-docstring
            return "_strategy_func_%s" % flow.name

        def strategy_func_import_name(flow):  # pylint: disable=missing-docstring
            return "_raw_strategy_func_%s" % flow.name

        strategy_dict = {}
        for flow in self.flows:
            output.write("from %s import %s as %s\n"
                         % (flow.strategy.module, flow.strategy.function, strategy_func_import_name(flow)))
            output.write('%s = functools.partial(%s, %s)\n\n'
                         % (strategy_func_name(flow),
                            strategy_func_import_name(flow),
                            dict2strkwargs(flow.strategy.func_args)))
            strategy_dict[flow.name] = strategy_func_name(flow)

        output.write('\n')
        self._dump_dict(output, 'strategies', strategy_dict)

    @staticmethod
    def _dump_condition_name(flow_name, idx):
        """Create condition name for a dump.

        :param flow_name: flow name
        :type flow_name: str
        :param idx: index of condition within the flow
        :type idx: int
        :return: condition function representation
        """
        assert idx >= 0  # nosec
        return '_condition_{}_{}'.format(flow_name, idx)

    @staticmethod
    def _dump_foreach_function_name(flow_name, idx):
        """Create foreach function name for a dump.

        :param flow_name: flow name
        :type flow_name: str
        :param idx: index of condition within the flow
        :type idx: int
        :return: condition function representation
        """
        assert idx >= 0  # nosec
        return '_foreach_{}_{}'.format(flow_name, idx)

    def _dump_condition_functions(self, output):
        """Dump condition functions to a stream.

        :param output: a stream to write to
        """
        for flow in self.flows:
            for idx, edge in enumerate(flow.edges):
                output.write('def {}(db, node_args):\n'.format(edge.predicate.construct_condition_name(flow.name, idx)))
                output.write('    return {}\n\n\n'.format(edge.predicate.to_source()))

            if flow.failures:
                flow.failures.dump_all_conditions2stream(output)

    def _dump_throttling(self, output):
        """Dump throttling configuration.

        :param output: a stream to write to
        """
        self._dump_dict(output, 'throttle_tasks', {t.name: repr(t.throttling) for t in self.tasks})
        self._dump_dict(output, 'throttle_flows', {f.name: repr(f.throttling) for f in self.flows})

    def _dump_max_retry(self, output):
        """Dump max_retry configuration to a stream.

        :param output: a stream to write to
        """
        output.write('max_retry = {')
        printed = False
        for node in self.tasks + self.flows:
            if printed:
                output.write(',')
            output.write("\n    '%s': %d" % (node.name, node.max_retry))
            printed = True
        output.write('\n}\n\n')

    def _dump_retry_countdown(self, output):
        """Dump retry_countdown configuration to a stream.

        :param output: a stream to write to
        """
        output.write('retry_countdown = {')
        printed = False
        for node in self.tasks + self.flows:
            if printed:
                output.write(',')
            output.write("\n    '%s': %d" % (node.name, node.retry_countdown))
            printed = True
        output.write('\n}\n\n')

    def _dump_storage_readonly(self, output):
        """Dump storage_readonly flow to a stream.

        :param output: a stream to write to
        """
        output.write('storage_readonly = {')
        printed = False
        for task in self.tasks:
            if printed:
                output.write(',')
            output.write("\n    '%s': %s" % (task.name, task.storage_readonly))
            printed = True
        output.write('\n}\n\n')

    def _dump_selective_run_functions(self, output):
        """Dump all selective run functions.

        :param output: a stream to write to
        """
        self._dump_dict(output,
                        'selective_run_task',
                        {t.name: t.selective_run_function.get_import_name() for t in self.tasks})

    def _dump_nowait_nodes(self, output):
        """Dump nowait nodes to a stream.

        :param output: a stream to write to
        """
        output.write('nowait_nodes = {\n')
        printed = False
        for flow in self.flows:
            if printed:
                output.write(',\n')
            output.write("    '%s': %s" % (flow.name, [node.name for node in flow.nowait_nodes]))
            printed = True

        output.write('\n}\n\n')

    def _dump_eager_failures(self, output):
        """Dump eager failures to a stream.

        :param output: a stream to write to
        """
        output.write('eager_failures = {\n')
        printed = False
        for flow in self.flows:
            if printed:
                output.write(',\n')
            output.write("    '%s': %s" % (flow.name, [node.name for node in flow.eager_failures]))
            printed = True

        output.write('\n}\n\n')

    @staticmethod
    def _dump_settings(output):
        """Dump generic settings that do not belong anywhere.

        :param output: a stream to write to
        """
        if GlobalConfig.migration_dir:
            output.write('migration_dir = %r\n' % GlobalConfig.migration_dir)
        else:
            output.write('migration_dir = None\n')

    @staticmethod
    def _dump_init(output):
        """Dump init function to a stream.

        :param output: a stream to write to
        :return:
        """
        output.write('def init(config_cls):\n')
        GlobalConfig.dump_trace(output, 'config_cls', indent_count=1)
        # Mark initialization as complete for the Config instance
        output.write('    config_cls.initialized = True\n')
        # always pass in case we have nothing to init
        output.write('    return\n')
        output.write('\n')

    def _dump_edge_table(self, output):
        """Dump edge definition table to a stream.

        :param output: a stream to write to
        """
        output.write('edge_table = {\n')
        for idx, flow in enumerate(self.flows):
            output.write("    '{}': [".format(flow.name))
            for idx_edge, edge in enumerate(flow.edges):
                if idx_edge > 0:
                    output.write(',\n')
                    output.write(' '*(len(flow.name) + 4 + 5))  # align to previous line
                output.write("{'from': %s" % str([node.name for node in edge.nodes_from]))
                output.write(", 'to': %s" % str([node.name for node in edge.nodes_to]))
                output.write(", 'condition': %s" % edge.predicate.construct_condition_name(flow.name, idx_edge))
                output.write(", 'condition_str': '%s'" % str(edge.predicate).replace('\'', '\\\''))
                if edge.foreach:
                    output.write(", 'foreach': %s" % self._dump_foreach_function_name(flow.name, idx_edge))
                    output.write(", 'foreach_str': '%s'" % edge.foreach_str())
                    output.write(", 'foreach_propagate_result': %s" % edge.foreach['propagate_result'])
                if edge.selective:
                    output.write(", 'selective': %s" % edge.selective)
                output.write("}")
            if idx + 1 < len(self.flows):
                output.write('],\n')
            else:
                output.write(']\n')
        output.write('}\n\n')

    def dump2stream(self, stream):
        """Perform system dump to a Python source code to an output stream.

        :param stream: an output stream to write to
        """
        # pylint: disable=too-many-statements
        stream.write('#!/usr/bin/env python3\n')
        stream.write('# auto-generated using Selinon v{} on {} at {}\n\n'.format(selinon_version,
                                                                                 platform.node(),
                                                                                 str(datetime.datetime.utcnow())))
        self._dump_imports(stream)
        self._dump_strategy_func(stream)
        self._dump_task_classes(stream)
        self._dump_storage_task_names(stream)
        self._dump_queues(stream)
        self._dump_storage_conf(stream)
        self._dump_async_result_cache(stream)
        self._dump_output_schemas(stream)
        self._dump_flow_flags(stream)
        self._dump_throttling(stream)
        self._dump_max_retry(stream)
        self._dump_retry_countdown(stream)
        self._dump_storage_readonly(stream)
        self._dump_selective_run_functions(stream)
        self._dump_nowait_nodes(stream)
        self._dump_eager_failures(stream)
        self._dump_init(stream)
        self._dump_condition_functions(stream)

        for flow in self.flows:
            if flow.failures:
                flow.failures.dump2stream(stream)

        stream.write('failures = {')
        printed = False
        for flow in self.flows:
            if flow.failures:
                if printed:
                    stream.write(",")
                printed = True
                stream.write("\n    '%s': %s" % (flow.name, flow.failures.starting_nodes_name(flow.name)))
        stream.write('\n}\n\n')

        self._dump_selective_run_functions(stream)
        self._dump_nowait_nodes(stream)
        self._dump_settings(stream)
        self._dump_init(stream)
        self._dump_condition_functions(stream)
        self._dump_edge_table(stream)

    def dump2file(self, output_file):
        """Perform system dump to a Python source code.

        :param output_file: an output file to write to
        """
        self._logger.debug("Performing system dump to '%s'", output_file)
        with open(output_file, 'w') as stream:
            self.dump2stream(stream)

    @staticmethod
    def _plot_connection(graph, node, condition_node, storage_connections, direction_to_condition=True, fallback=False):
        """Plot node connection to graph.

        :param graph: graph to plot to
        :param node: node to plot
        :param condition_node: condition node to connect to
        :param storage_connections: storages that were plotted with their connections (not to plot duplicit edges)
        :param direction_to_condition: direction of the connection
        :param fallback: True if plotting a fallback edge
        """
        # pylint: disable=too-many-arguments
        edge_style = UserConfig().style_fallback_edge() if fallback else UserConfig().style_edge()

        if node.is_flow():
            graph.node(name=node.name, _attributes=UserConfig().style_flow())
        else:
            graph.node(name=node.name)
            if node.storage:
                graph.node(name=node.storage.name, _attributes=UserConfig().style_storage())
                if (node.name, node.storage.name) not in storage_connections:
                    graph.edge(node.name, node.storage.name, _attributes=UserConfig().style_store_edge())
                    storage_connections.append((node.name, node.storage.name,))

        if direction_to_condition:
            graph.edge(node.name, condition_node, _attributes=edge_style)
        else:
            graph.edge(condition_node, node.name, _attributes=edge_style)

    def _plot_graph_failures(self, graph, flow, storage_connections):
        """Plot failures to existing graph.

        :param graph: graph to plot to
        :param flow: flow which failures should be plotted
        :param storage_connections: storage connections that were already defined
        """
        for idx, failure in enumerate(flow.failures.raw_definition):
            condition = flow.failures.predicates[idx]
            # This is kind-of-hack-ish as we do not want to traverse all failure nodes, so construct some unique name
            # for this particular failure condition to ensure we do not overwrite an existing one
            # See FailureNode.construct_condition_name() for a "proper" naming
            condition_node = flow.name + str(idx)
            graph.node(name=condition_node, label=str(condition), _attributes=UserConfig().style_condition())

            for node_name in failure['nodes']:
                node = self.node_by_name(node_name)
                self._plot_connection(graph, node, condition_node, storage_connections,
                                      direction_to_condition=True, fallback=True)

            if isinstance(failure['fallback'], bool):
                graph.node(name=str(id(failure['fallback'])), label=str(failure['fallback']),
                           _attributes=UserConfig().style_fallback_true())
                graph.edge(condition_node, str(id(failure['fallback'])), _attributes=UserConfig().style_fallback_edge())
            else:
                for node_name in failure['fallback']:
                    node = self.node_by_name(node_name)
                    self._plot_connection(graph, node, condition_node, storage_connections,
                                          direction_to_condition=False, fallback=True)

    def plot_graph(self, output_dir, image_format=None):  # pylint: disable=too-many-statements,too-many-branches
        """Plot system flows to graphs - each flow in a separate file.

        :param output_dir: output directory to write graphs of flows to
        :param image_format: image format, the default is svg if None
        :return: list of file names to which the graph was rendered
        :rtype: List[str]
        """
        self._logger.debug("Rendering system flows to '%s'", output_dir)
        ret = []
        image_format = image_format if image_format else 'svg'

        for flow in self.flows:
            storage_connections = []
            graph = graphviz.Digraph(format=image_format)
            graph.graph_attr.update(UserConfig().style_graph())
            graph.node_attr.update(UserConfig().style_task())
            graph.edge_attr.update(UserConfig().style_edge())

            for idx, edge in enumerate(flow.edges):
                condition_node = edge.predicate.construct_condition_name(flow.name, idx)
                if edge.foreach:
                    condition_label = "%s\n%s" % (str(edge.predicate), edge.foreach_str())
                    graph.node(name=condition_node, label=condition_label,
                               _attributes=UserConfig().style_condition_foreach())
                else:
                    graph.node(name=condition_node, label=str(edge.predicate),
                               _attributes=UserConfig().style_condition())

                for node in edge.nodes_from:
                    self._plot_connection(graph, node, condition_node, storage_connections,
                                          direction_to_condition=True)
                for node in edge.nodes_to:
                    self._plot_connection(graph, node, condition_node, storage_connections,
                                          direction_to_condition=False)

            # Plot failures as well
            if flow.failures:
                self._plot_graph_failures(graph, flow, storage_connections)

            file = os.path.join(output_dir, "%s" % flow.name)
            graph.render(filename=file, cleanup=True)
            ret.append(file)
            self._logger.info("Graph rendered to '%s.%s'", file, image_format)

        return ret

    def _post_parse_check(self):
        """Check parsed definition.

        Called once parse was done to ensure that system was correctly defined in config file.
        :raises: ValueError
        """
        self._logger.debug("Post parse check is going to be executed")
        # we want to have circular dependencies, so we need to check consistency after parsing since all flows
        # are listed (by names) in a separate definition
        for flow in self.flows:
            if not flow.edges:
                raise ConfigurationError("Empty flow: %s" % flow.name)

    def _check_propagate(self, flow):  # pylint: disable=too-many-branches
        """Check propagate configuration.

        :param flow: flow that should be checked
        :type flow: Flow
        :raises ValueError: if propagate check fails
        """
        all_source_nodes = flow.all_source_nodes()
        all_destination_nodes = flow.all_destination_nodes()
        #
        # checks on propagate_{compound_,}finished
        #
        if isinstance(flow.propagate_finished, list):
            for node in flow.propagate_finished:
                if node not in all_source_nodes:
                    raise ConfigurationError("Subflow '%s' should receive parent nodes, but there is no dependency "
                                             "in flow '%s' to which should be parent nodes propagated"
                                             % (node.name, flow.name))

                # propagate_finished set to a flow but these arguments are not passed due
                # to propagate_parent
                if node.is_flow():
                    affected_edges = [edge for edge in flow.edges if node in edge.nodes_from]
                    for affected_edge in affected_edges:
                        f = [n for n in affected_edge.nodes_to if n.is_flow()]  # pylint: disable=invalid-name
                        if len(f) == 1 and not flow.should_propagate_parent(f[0]):
                            self._logger.warning("Flow '%s' marked in propagate_finished, but calculated "
                                                 "finished nodes are not passed to sub-flow '%s' due to not "
                                                 "propagating parent, in flow '%s'",
                                                 node.name, f[0].name, flow.name)

        if isinstance(flow.propagate_compound_finished, list):
            for node in flow.propagate_compound_finished:
                if node not in all_source_nodes:
                    raise ConfigurationError("Subflow '%s' should receive parent nodes, but there is no dependency "
                                             "in flow '%s' to which should be parent nodes propagated"
                                             % (node.name, flow.name))

                # propagate_compound_finished set to a flow but these arguments are not passed due
                # to propagate_parent
                if node.is_flow():
                    affected_edges = [edge for edge in flow.edges if node in edge.nodes_from]
                    for affected_edge in affected_edges:
                        f = [n for n in affected_edge.nodes_to if n.is_flow()]  # pylint: disable=invalid-name
                        if len(f) == 1 and not flow.should_propagate_parent(f[0]):
                            self._logger.warning("Flow '%s' marked in propagate_compound_finished, but "
                                                 "calculated finished nodes are not passed to sub-flow '%s' "
                                                 "due to not propagating parent, in flow '%s'",
                                                 node.name, f[0].name, flow.name)
        #
        # checks on propagate_{compound_,}failures
        #
        # TODO: check there is set propagate_parent_failures and there is a fallback subflow to handle flow failures
        all_waiting_failure_nodes = flow.failures.all_waiting_nodes() if flow.failures else []
        if isinstance(flow.propagate_failures, list):
            for node in flow.propagate_failures:
                if node not in all_source_nodes:
                    raise ConfigurationError("Node '%s' stated in propagate_failures but this node is not started "
                                             "in flow '%s'" % (node.name, flow.name))
                if node not in all_waiting_failure_nodes:
                    raise ConfigurationError("Node '%s' stated in propagate_failures but there is no such fallback "
                                             "defined that would handle node's failure in flow '%s'"
                                             % (node.name, flow.name))

        if isinstance(flow.propagate_compound_failures, list):
            for node in flow.propagate_compound_failures:
                if node not in all_source_nodes:
                    raise ConfigurationError("Node '%s' stated in propagate_compound_failures but this node is "
                                             "not started in flow '%s'" % (node.name, flow.name))
                if node not in all_waiting_failure_nodes:
                    raise ConfigurationError("Node '%s' stated in propagate_compound_failures but there is "
                                             "no such fallback defined that would handle node's failure in flow '%s'"
                                             % (node.name, flow.name))

        if isinstance(flow.propagate_parent, list):
            for node in flow.propagate_parent:
                if node not in all_destination_nodes:
                    raise ConfigurationError("Subflow '%s' should receive parent, but there is no dependency "
                                             "in flow '%s' to which should be parent nodes propagated"
                                             % (node.name, flow.name))

        if isinstance(flow.propagate_finished, list) and isinstance(flow.propagate_compound_finished, list):
            for node in flow.propagate_finished:
                if node in flow.propagate_compound_finished:
                    raise ConfigurationError("Cannot mark node '%s' for propagate_finished and "
                                             "propagate_compound_finished at the same time in flow '%s'"
                                             % (node.name, flow.name))
        else:
            if (flow.propagate_finished is True and flow.propagate_compound_finished is True)  \
                  or (flow.propagate_finished is True and isinstance(flow.propagate_compound_finished, list)) \
                  or (isinstance(flow.propagate_finished, list) and flow.propagate_compound_finished is True):
                raise ConfigurationError("Flags propagate_compound_finished and propagate_finished are disjoint,"
                                         " please specify configuration for each node separately in flow '%s'"
                                         % flow.name)

        if isinstance(flow.propagate_failures, list) and isinstance(flow.propagate_compound_failures, list):
            for node in flow.propagate_failures:
                if node in flow.propagate_compound_failures:
                    raise ConfigurationError("Cannot mark node '%s' for propagate_failures and "
                                             "propagate_compound_failures at the same time in flow '%s'"
                                             % (node.name, flow.name))
        else:
            if (flow.propagate_failures is True and flow.propagate_compound_failures is True)  \
                  or (flow.propagate_failures is True and isinstance(flow.propagate_compound_failures, list)) \
                  or (isinstance(flow.propagate_failures, list) and flow.propagate_compound_failures is True):
                raise ConfigurationError("Flags propagate_compound_failures and propagate_failures are disjoint, "
                                         "please specify configuration for each node separately in flow '%s'"
                                         % flow.name)

    def _check(self):  # pylint: disable=too-many-statements,too-many-branches
        """Check system for consistency.

        :raises: ValueError
        """
        self._logger.info("Checking system consistency")

        for task_class in self.task_classes:
            task_ref = task_class.tasks[0]
            for task in task_class.tasks[1:]:
                if task_ref.output_schema != task.output_schema:
                    self._logger.warning("Different output schemas to a same task class: %s and %s for class '%s', "
                                         "schemas: '%s' and '%s' might differ",
                                         task_ref.name, task.name, task_class.class_name,
                                         task_ref.output_schema, task.output_schema)

                if task.max_retry != task_ref.max_retry:
                    self._logger.warning("Different max_retry assigned to a same task class: %s and %s for class '%s' "
                                         "(import '%s')",
                                         (task.name, task.max_retry), (task_ref.name, task_ref.max_retry),
                                         task_class.class_name, task_class.import_path)

        for storage in self.storages:
            if not storage.tasks:
                self._logger.warning("Storage '%s' not used in any flow", storage.name)

        all_used_nodes = set()
        # We want to check that if we depend on a node, that node is being started at least once in the flow
        # This also covers check for starting node definition
        for flow in self.flows:
            # TODO: we should make this more transparent by placing it to separate functions
            try:
                all_source_nodes = flow.all_source_nodes()
                all_destination_nodes = flow.all_destination_nodes()

                starting_edges_count = 0
                starting_nodes_count = 0

                for edge in flow.edges:
                    if not edge.nodes_from:
                        starting_edges_count += 1
                        starting_nodes_count += len(edge.nodes_to)

                        if flow.node_args_from_first:
                            if len(edge.nodes_to) > 1:
                                raise ConfigurationError("Cannot propagate node arguments from multiple "
                                                         "starting nodes")

                            if edge.nodes_to[0].is_flow():
                                raise ConfigurationError("Cannot propagate node arguments from a sub-flow")

                    node_seen = {}
                    for node_from in edge.nodes_from:
                        if not node_seen.get(node_from.name, False):
                            node_seen[node_from.name] = True
                        else:
                            raise ConfigurationError("Nodes cannot be dependent on a node of a same type mode "
                                                     "than once; node from '%s' more than once in flow '%s'"
                                                     % (node_from.name, flow.name))

                    # do not change order of checks as they depend on each other
                    edge.predicate.check()
                    edge.check()

                if starting_edges_count > 1:
                    if flow.node_args_from_first:
                        raise ConfigurationError("Cannot propagate node arguments from multiple starting nodes")

                if starting_nodes_count == 0:
                    raise ConfigurationError("No starting node found in flow '%s'" % flow.name)

                for nowait_node in flow.nowait_nodes:
                    if nowait_node in all_source_nodes:
                        raise ConfigurationError("Node '%s' marked as 'nowait' but dependency in the flow '%s' found"
                                                 % (nowait_node.name, flow.name))

                    if nowait_node not in flow.all_destination_nodes():
                        raise ConfigurationError("Node '%s' marked as 'nowait' but this node is never started in "
                                                 "flow '%s'" % (nowait_node.name, flow.name))

                for node in flow.eager_failures if isinstance(flow.eager_failures, list) else []:
                    if node not in flow.all_used_nodes():
                        raise ConfigurationError("Node %r marked as eager failure node in flow %r, but "
                                                 "this node is not present in the flow" % (node.name, flow.name))
                    if node in flow.nowait_nodes:
                        raise ConfigurationError("Node %r marked as eager failure node in flow %r, but "
                                                 "this node is also present in nowait nodes, thus it's result "
                                                 "cannot be be inspected" % (node.name, flow.name))

                    if node in flow.failures.all_waiting_nodes():
                        raise ConfigurationError("Node %r marked as eager failure node in flow %r, but "
                                                 "this node has defined a fallback, thus it cannot stop "
                                                 "flow execution eagerly" % (node.name, flow.name))

                self._check_propagate(flow)

                all_used_nodes = set(all_used_nodes) | set(all_source_nodes) | set(all_destination_nodes)
                not_started = list(set(all_source_nodes) - set(all_destination_nodes))

                error = False
                for node in not_started:
                    if node.is_task():
                        self._logger.error("Dependency in flow '%s' on node '%s', but this node is not started "
                                           "in the flow", flow.name, node.name)
                        error = True

                if error:
                    raise ConfigurationError("Dependency on not started node detected in flow '%s'" % flow.name)
            except:
                self._logger.error("Check of flow '%s' failed", flow.name)
                raise

        # we report only tasks that are not run from any flow, running a flow is based on the user
        # this check could be written more optimal by storing only tasks, but keep it this way for now
        never_started_nodes = set(self.tasks) - set(t for t in all_used_nodes if t.is_task())
        for node in never_started_nodes:
            self._logger.warning("Task '%s' (class '%s' from '%s') stated in the YAML configuration file, but "
                                 "never run in any flow", node.name, node.class_name, node.import_path)

    @classmethod
    def _setup_nodes(cls, system, nodes_definition, nodes_definition_file_name):
        """Configure nodes available in the system based on supplied configuration.

        :param system: system instance to be used
        :type system: selinon.system.System
        :param nodes_definition: a list of dictionaries holding flow configuration
        :type nodes_definition: dict
        :param nodes_definition_file_name: a name of nodes definition file (used in messages for better debugging)
        :type nodes_definition_file_name: str
        """
        def append_file_name_if_any(error_message):
            """Append file name to an error message."""
            if nodes_definition_file_name:
                error_message += ", in file %r" % nodes_definition_file_name
            return error_message

        # known top-level YAML keys for YAML config files (note flows.yml could be merged to nodes.yml)
        known_yaml_keys = ('tasks', 'flows', 'storages', 'global', 'flow-definitions')

        unknown_conf = check_conf_keys(nodes_definition, known_conf_opts=known_yaml_keys)
        if unknown_conf:
            cls._logger.warning("Unknown configuration keys in the nodes definitions, "
                                "will be skipped: %s", list(unknown_conf.keys()))

        for storage_dict in nodes_definition.get('storages', []):
            storage = Storage.from_dict(storage_dict)
            system.add_storage(storage)

        if 'global' in nodes_definition:
            GlobalConfig.from_dict(system, nodes_definition['global'])

        if 'tasks' not in nodes_definition or nodes_definition['tasks'] is None:
            raise ConfigurationError(append_file_name_if_any("No tasks defined in the system"))

        for task_dict in nodes_definition['tasks']:
            task = Task.from_dict(task_dict, system)
            task_class = system.class_of_task(task)
            if not task_class:
                task_class = TaskClass(task.class_name, task.import_path)
                system.task_classes.append(task_class)
            task_class.add_task(task)
            task.task_class = task_class
            system.add_task(task)

        if 'flows' not in nodes_definition or nodes_definition['flows'] is None:
            raise ConfigurationError(append_file_name_if_any("No flow listing defined in the system"
                                                             "in nodes definition"))

        for flow_name in nodes_definition['flows']:
            flow = Flow(flow_name)
            system.add_flow(flow)

    @classmethod
    def _setup_flows(cls, system, flow_definitions, flow_definition_file_name=None):
        """Configure Flow instances based on supplied configuration.

        :param system: system instance to be used
        :type system: selinon.system.System
        :param flow_definitions: a list of dictionaries holding flow configuration
        :type flow_definitions: list
        :param flow_definition_file_name: a name of flow definition file (used in messages for better debugging)
        :type flow_definition_file_name: str
        """
        def append_file_name_if_any(error_message):
            """Append file name to an error message."""
            if flow_definition_file_name:
                error_message += ", in file %r" % flow_definition_file_name
            return error_message

        if not isinstance(flow_definitions, (list, tuple)):
            flow_definitions = (flow_definitions,)

        for content in flow_definitions:
            flow_definitions = content.get('flow-definitions')
            if flow_definitions is None:
                error_msg = append_file_name_if_any("No flow-definitions provided in flow specification")
                raise ConfigurationError(error_msg)

            for flow_def in content['flow-definitions']:
                if 'name' not in flow_def:
                    error_msg = append_file_name_if_any("No flow name provided in the flow definition")
                    raise ConfigurationError(error_msg)

                flow = system.flow_by_name(flow_def['name'])
                try:
                    flow.parse_definition(flow_def, system)
                except:
                    error_msg = append_file_name_if_any("Failed to parse flow definition for flow %r"
                                                        % flow_def['name'])
                    cls._logger.error(error_msg)
                    raise

    @classmethod
    def from_files(cls, nodes_definition_file, flow_definition_files, no_check=False):
        """Construct System from files.

        :param nodes_definition_file: path to nodes definition file
        :type nodes_definition_file: str
        :param flow_definition_files: path to files that describe flows
        :type flow_definition_files: str
        :param no_check: True if system shouldn't be checked for consistency (recommended to check)
        :return: System instance
        :rtype: System
        """
        system = System()

        with open(nodes_definition_file, 'r') as nodes_file:
            cls._logger.debug("Parsing '%s'", nodes_definition_file)
            try:
                nodes_definition = yaml.load(nodes_file, Loader=yaml.SafeLoader)
            except Exception as exc:
                error_msg = "Bad YAML file, unable to load %r: %s" % (nodes_definition_file, str(exc))
                raise ConfigurationError(error_msg) from exc

            cls._setup_nodes(system, nodes_definition, nodes_definition_file)

        if isinstance(flow_definition_files, str):
            flow_definition_files = (flow_definition_files,)

        for flow_file in flow_definition_files:
            with open(flow_file, 'r') as flow_definition:
                cls._logger.debug("Parsing '%s'", flow_file)
                try:
                    content = yaml.load(flow_definition, Loader=yaml.SafeLoader)
                except Exception as exc:
                    error_msg = "Bad YAML file, unable to load flow from %r: %s" % (flow_file, str(exc))
                    raise ConfigurationError(error_msg) from exc

                cls._setup_flows(system, content, flow_file)

        system._post_parse_check()  # pylint: disable=protected-access
        if not no_check:
            system._check()  # pylint: disable=protected-access

        return system

    @classmethod
    def from_dict(cls, nodes_definition, flow_definitions, no_check=False):
        """Construct System from dictionaries.

        :param nodes_definition: parsed nodes definition
        :type nodes_definition: dict
        :param flow_definitions: a list of parsed flow definitions
        :type nodes_definition: list
        :param no_check: True if system shouldn't be checked for consistency (recommended to check)
        :return: System instance
        :rtype: System
        """
        system = System()

        cls._setup_nodes(system, nodes_definition, nodes_definition_file_name=None)

        for flow_def in flow_definitions:
            cls._setup_flows(system, flow_def, flow_definition_file_name=None)

        system._post_parse_check()  # pylint: disable=protected-access
        if not no_check:
            system._check()  # pylint: disable=protected-access

        return system
