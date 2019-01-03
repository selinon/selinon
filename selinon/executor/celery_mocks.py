#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Injected Celery related implementations of methods.

Classes and functions to make Selinon executor work as a standalone CLI.
"""


class SimulateRequest:
    """Simulate Celery's Task.request.

    Make possible to query task id right inside task by calling self.request.id
    """

    def __init__(self, instance):
        """Instantiate request.

        :param instance:Instance for the request.
        """
        self.id = str(id(instance))  # pylint: disable=redefined-builtin,invalid-name


class SimulateAsyncResult:
    """Simulate AsyncResult returned by apply_async() or by instantiating AsyncResult by Task id."""

    task_failures = {}
    task_successes = {}

    def __init__(self, node_name, node_id):  # pylint: disable=redefined-builtin,invalid-name
        """Initialize AsyncResult.

        :param node_name: name of the node for which the AsyncResult should be stored
        :param node_id: ID of node for which the AsyncResult should be stored
        """
        self.task_id = str(node_id)
        self.node_id = node_name

    @classmethod
    def set_successful(cls, task_id, result):
        """Mark task as successful after run - called by executor.

        :param task_id: an ID of task that should be marked as successful
        :param result: result of task (None for SelinonTaskEnvelope, JSON describing system state for Dispatcher)
        """
        cls.task_successes[task_id] = result

    @classmethod
    def set_failed(cls, task_id, exception):
        """Mark task as failed after run - called by executor.

        :param task_id: an ID of task that should be marked as failed
        :param exception: exception raised in task
        """
        cls.task_failures[task_id] = exception

    def successful(self):
        """Check for success.

        :return: True if task succeeded.
        """
        return self.task_id in self.task_successes

    def failed(self):
        """Check for failure.

        :return: True if task failed
        """
        return self.task_id in self.task_failures

    @property
    def traceback(self):
        """Traceback as returned by Celery's AsyncResult.

        :return: traceback returned by a task
        """
        return self.task_failures[self.task_id]

    @property
    def result(self):
        """Get result of a task.

        :return: retrieve result of the task or exception that was raised
        """
        return self.task_successes.get(self.task_id, None)


class SimulateRetry(Exception):
    """Simulate Celery Retry exception raised by self.retry()."""

    def __init__(self, instance, **celery_kwargs):
        """Instantiate Retry exception.

        :param instance: instance that triggered retry
        :param celery_kwargs: kwargs arguments as passed to raw Celery task
        """
        super().__init__()
        self.instance = instance
        self.celery_kwargs = celery_kwargs


def simulate_apply_async(instance, **celery_kwargs):
    """Simulate CeleryTask().apply_async() implementation for scheduling tasks.

    :param instance: instance that should be scheduled
    :param celery_kwargs: kwargs supplied to Celery Task (also carry arguments for Selinon)
    """
    from .executor import Executor

    instance.request = SimulateRequest(instance)
    Executor.schedule(instance, celery_kwargs)
    selinon_kwargs = celery_kwargs['kwargs']
    return SimulateAsyncResult(selinon_kwargs.get('task_name', selinon_kwargs['flow_name']),
                               node_id=id(instance))


def simulate_retry(instance, **celery_kwargs):
    """Simulate Celery self.retry() implementation for retrying tasks.

    :param instance: instance that should called self.retry()
    :param celery_kwargs: kwargs that will be supplied to Celery Task (also carry arguments for Selinon)
    """
    raise SimulateRetry(instance, **celery_kwargs)
