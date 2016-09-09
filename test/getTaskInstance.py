#!/usr/bin/env python3
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


# a custom task is used, see ./test/celery/task.py
from celery import Task
from celery.result import AsyncResult


class GetTaskInstance(object):
    """
    Task instance factory used in system state to instantiate tasks
    """
    _flow_instances = []

    def __init__(self, task_instances=None):
        self._task_instances = task_instances if task_instances else []

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
    def register_flow(cls, flow):
        """
        Register flow that has started
        :param flow: flow to be tracked
        """
        cls._flow_instances.append(flow)
        AsyncResult.set_unfinished(flow.task_id)

    @classmethod
    def clear(cls):
        """
        Since we are using class attributes and methos, we need to clear each time there will be a test
        :return:
        """
        cls._flow_instances = []

    def __call__(self, task_name, flow_name, parent, args, finished, retried_count):
        """
        Instantiate a task with name task_name
        :param task_name: a name of the task to be instantiated
        :return: task instance
        """
        task_instance = Task(task_name, flow_name, parent, args, finished, retried_count)
        self._task_instances.append(task_instance)
        AsyncResult.set_unfinished(task_instance.task_id)
        return task_instance
