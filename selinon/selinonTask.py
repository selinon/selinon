#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Base class for user-defined tasks."""

import abc
import logging

from celery.result import AsyncResult
from selinonlib import NoParentNodeError
from selinonlib import RequestError
from selinonlib import Retry

from .storagePool import StoragePool


class SelinonTask(metaclass=abc.ABCMeta):
    """Base class for user-defined tasks."""

    def __init__(self, flow_name, task_name, parent, task_id, dispatcher_id):
        """Initialize SelinonTask (called by SelinonTaskEnvelope).

        :param flow_name: name of flow under which this tasks runs on
        :param task_name: name of task, note it can be aliased since we can have different task name and class name
        :param parent: direct task's predecessors stated in flow dependency
        :param task_id: id of this task
        :parent dispatcher_id: id of dispatcher handling the current flow
        """
        # pylint: disable=too-many-arguments
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent
        self.task_id = task_id
        self.dispatcher_id = dispatcher_id
        self.log = logging.getLogger(__name__)

    def _selinon_dereference_task_id(self, flow_names, task_name, index):
        """Compute task id based on mapping of ancestors (from parent sub-flows).

        :param flow_names: name of parent flow or list of flow names in case of nested flows
        :param task_name: name of task in parent flow
        :param index: index of result if more than one subflow was run
        :return: task id based from parent subflows
        """
        if not isinstance(flow_names, list):
            flow_names = [flow_names]

        parent_flow = self.parent
        for flow_name in flow_names:
            try:
                parent_flow = parent_flow[flow_name]
            except KeyError as exc:
                raise NoParentNodeError("No such parent flow '%s' for task '%s', check your configuration; nested "
                                        "as %s from flow %s"
                                        % (flow_name, self.task_name, flow_names, self.flow_name)) from exc
        try:
            task_id = parent_flow[task_name][index]
        except KeyError as exc:
            raise NoParentNodeError("No such parent task '%s' referenced by '%s' was run for task '%s' in flow '%s'"
                                    % (task_name, flow_names, self.task_name, self.flow_name)) from exc
        except IndexError as exc:
            raise NoParentNodeError("Requested index %s in parent task '%s' referencered by '%s', but there were "
                                    "run only %d tasks in task %s flow %s"
                                    % (index, task_name, flow_names, len(parent_flow[task_name]), self.task_name,
                                       self.flow_name)) from exc
        return task_id

    @property
    def storage(self):
        """Storage instance assigned to this task.

        :return: tasks's configured storage as stated in YAML config
        """
        return StoragePool.get_storage_by_task_name(self.task_name)

    def parent_task_result(self, parent_name):
        """Retrieve parent task result.

        :param parent_name: name of parent task to retrieve result from
        :return: result of parent task
        """
        try:
            parent_task_id = self.parent[parent_name]
        except KeyError as exc:
            raise NoParentNodeError("No such parent '%s' in task '%s' in flow '%s', check your configuration"
                                    % (parent_name, self.task_name, self.flow_name)) from exc

        return StoragePool.retrieve(self.flow_name, parent_name, parent_task_id)

    def parent_flow_result(self, flow_names, task_name, index=None):
        """Retrieve result of parent sub-flow task.

        Get parent subflow results; note that parent flows can return multiple results from task of same type
        because of loops in flows

        :param flow_names: name of parent flow or list of flow names in case of nested flows
        :param task_name: name of task in parent flow
        :param index: index of result if more than one subflow was run
        :return: result of task in parent subflow
        """
        index = -1 if index is None else index
        parent_flow_name = flow_names if not isinstance(flow_names, list) else flow_names[-1]
        task_id = self._selinon_dereference_task_id(flow_names, task_name, index)
        return StoragePool.retrieve(parent_flow_name, task_name, task_id)

    def parent_task_exception(self, parent_name):
        """Retrieve parent task exception. You have to call this from a fallback (direct or transitive).

        :param parent_name: name of task that failed (ancestor of calling task)
        :return exception that was raised in the ancestor
        """
        try:
            parent_task_id = self.parent[parent_name]
        except KeyError as exc:
            raise NoParentNodeError("No such parent '%s' in task '%s' in flow '%s', check your configuration"
                                    % (parent_name, self.task_name, self.flow_name)) from exc

        # Celery stores exceptions in result field
        celery_result = AsyncResult(parent_task_id)
        if not celery_result.failed():
            raise RequestError("Parent task '%s' did not raised exception" % parent_name)

        return celery_result.result

    def parent_flow_exception(self, flow_names, task_name, index=None):
        """Retrieve parent task exception. You have to call this from a fallback (direct or transitive).

        :param flow_names: name of parent flow or list of flow names in case of nested flows
        :param task_name: name of task that failed (ancestor of calling task)
        :param index: index of result if more than one subflow was run
        :return exception that was raised in the ancestor
        """
        index = -1 if index is None else index
        task_id = self._selinon_dereference_task_id(flow_names, task_name, index)

        # Celery stores exceptions in result field
        celery_result = AsyncResult(task_id)
        if not celery_result.failed():
            raise RequestError("Parent task '%s' from subflows %s did not raised exception"
                               % (task_name, str(flow_names)))

        return celery_result.result

    @staticmethod
    def retry(countdown=0):
        """Retry, always raises Retry, this is compatible with Celery's retry except you cannot modify arguments.

        :param countdown: countdown for rescheduling
        """
        raise Retry(countdown)

    @abc.abstractmethod
    def run(self, node_args):
        """Entrypoint - user defined computation.

        :param node_args: arguments passed to flow/node
        :return: tasks's result that will be stored in database as configured
        """
        pass
