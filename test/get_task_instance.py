#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


class GetTaskInstance:
    """
    Task instance factory used in system state to instantiate tasks
    """
    _flow_instances = []
    _task_instances = []

    @property
    def task_instances(self):
        """
        :return: all task instances that were instantiated
        """
        return self._task_instances

    @property
    def tasks(self):
        """
        :return: task names of tasks that were instantiated
        """
        return [t.task_name for t in self._task_instances]

    @property
    def flows(self):
        """
        :return: flow names of flows that were instantiated
        """
        return [f.flow_name for f in self._flow_instances]

    def task_by_name(self, task_name):
        """
        :param task_name: task name to look up
        :return: all tasks that were started with name task_name
        """
        return [t for t in self._task_instances if t.task_name == task_name]

    def flow_by_name(self, flow_name):
        """
        :param flow_name: flow name to look up
        :return: all flows that were started with name flow_name
        """
        return [f for f in self._flow_instances if f.flow_name == flow_name]

    @classmethod
    def register_node(cls, node):
        """
        Register a node that was scheduled
        :param node: flow to be tracked
        """
        # a custom task is used, see ./test/celery/task.py
        from celery.result import AsyncResult
        from selinon.dispatcher import Dispatcher

        if isinstance(node, Dispatcher):
            cls._flow_instances.append(node)
        else:
            cls._task_instances.append(node)
        AsyncResult.set_unfinished(node.task_id)

    @classmethod
    def remove_all_flows_by_name(cls, flow_name):
        """ Remove all flows with the given name """
        cls._flow_instances = [f for f in cls._flow_instances if f.flow_name != flow_name]

    @classmethod
    def remove_all_tasks_by_name(cls, task_name):
        """ Remove all flows with the given name """
        cls._task_instances = [t for t in cls._task_instances if t.task_name != task_name]

    @classmethod
    def clear(cls):
        """
        Since we are using class attributes and methos, we need to clear each time there will be a test
        :return:
        """
        cls._flow_instances = []
        cls._task_instances = []

