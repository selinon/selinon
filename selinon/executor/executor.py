#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Simulate execution in a single CLI run.

Using Selinon executor is a good way to test you configuration and behaviour locally if you would like to save
some time when debugging or exploring all the possibilities that Selinon offers you. Keep in mind that running
Selinon locally was designed for development purposes and behaviour in general can (and in many cases will) vary.

The key idea behind executor is to simulate exchange of messages which is done by your Celery broker and Kombu under
the hood of Celery. Thus there are lazily created queues that are referenced by their names (see
selinon.executor.time_queue for implementation details). These queues hold messages prior to time on which they were
scheduled. Under the hood there is used a heap queue to optimize inserting to O(log(N)) in the worst case where N is
number messages currently in the queue. These queues are coupled into QueuePool (selinon.execute.queue_pool) which
encapsulates all queues, keeps their references, instantiates it lazily and provides concurrency safety.

In order to avoid starving, QueuePool keeps track of the queue ("a last used queue") which prevents from
starving messages that were scheduled for the same time (basically a simple round-robin). QueuePool looks for a message
that is not scheduled to the future and can be executed (to do so we get O(N) where N is number of queues being used).

All workers listen on all queues for now. This prevents from waiting on a message that would be never processed.

In order to understand how Executor works, you need to understand how Celery works. Please refer to Celery
documentation if you are a Celery-newbie.
"""

import copy
from datetime import datetime
from datetime import timedelta
import logging
import traceback

from selinon.celery import Task as CeleryTask
from selinon import Config
from selinon import run_flow
from selinon import run_flow_selective
from selinon.system_state import SystemState
from selinon import UnknownError
from selinon.global_config import GlobalConfig

from .celery_mocks import simulate_apply_async
from .celery_mocks import simulate_retry
from .celery_mocks import SimulateAsyncResult
from .celery_mocks import SimulateRetry
from .progress import Progress
from .queue_pool import QueuePool


class Executor:
    """Executor that executes Selinon run in a multi-process environment."""

    executor_queues = QueuePool()
    _logger = logging.getLogger(__name__)

    DEFAULT_SLEEP_TIME = 1
    DEFAULT_CONCURRENCY = 1

    def __init__(self, nodes_definition, flow_definitions,
                 concurrency=DEFAULT_CONCURRENCY, sleep_time=DEFAULT_SLEEP_TIME,
                 config_py=None, keep_config_py=False, show_progressbar=True):
        """Instantiate execute.

        :param nodes_definition: path to nodes.yaml file
        :type nodes_definition: str
        :param flow_definitions: a list of YAML files describing flows
        :type flow_definitions: list
        :param concurrency: executor concurrency
        :type concurrency: int
        :param sleep_time: number of seconds to wait before querying queue
        :type sleep_time: float
        :param config_py: a path to file where Python code configuration should be generated
        :type config_py: str
        :param keep_config_py: if true, do not delete generated config.py
        :type keep_config_py: bool
        :param show_progressbar: show progressbar on executor run
        :type show_progressbar: bool
        """
        Config.set_config_yaml(nodes_definition, flow_definitions,
                               config_py=config_py,
                               keep_config_py=keep_config_py)

        self.concurrency = concurrency
        self.sleep_time = sleep_time
        self.show_progressbar = show_progressbar

        if concurrency != 1:
            raise NotImplementedError("Concurrency is now unsupported")

    @staticmethod
    def _prepare():
        """Prepare Selinon for executor run."""
        # We need to assign a custom async result as we are not running Celery but our mocks instead
        SystemState._get_async_result = SimulateAsyncResult  # pylint: disable=protected-access
        # Overwrite used Celery functions so we do not rely on Celery logic at all
        CeleryTask.apply_async = simulate_apply_async
        CeleryTask.retry = simulate_retry

    def run(self, flow_name, node_args=None):
        """Run executor.

        :param flow_name: a flow name that should be run
        :param node_args: arguments for the flow
        """
        self._prepare()
        run_flow(flow_name, node_args)
        self._executor_run()

    def run_flow_selective(self, flow_name, task_names, node_args=None, follow_subflows=False, run_subsequent=False):
        """Run only desired tasks in a flow.

        :param flow_name: name of the flow that should be run
        :param task_names: name of the tasks that should be run
        :param node_args: arguments that should be supplied to flow
        :param follow_subflows: if True, subflows will be followed and checked for nodes to be run
        :param run_subsequent: trigger run of all tasks that depend on the desired task
        :return: dispatcher id that is scheduled to run desired selective task flow
        :raises selinon.errors.SelectiveNoPathError: there was no way found to the desired task in the flow
        """
        self._prepare()
        run_flow_selective(
            flow_name,
            task_names,
            node_args,
            follow_subflows,
            run_subsequent
        )
        self._executor_run()

    def _executor_run(self):
        """Perform task execution based on published message on queue."""
        while not self.executor_queues.is_empty():
            # TODO: concurrency
            self._logger.debug("new executor run")

            # Retrieve a task that can be run right now
            time, record = self.executor_queues.pop()
            task, celery_kwargs = record

            # we got a task with the lowest wait time - we need to wait if the task was scheduled in the future
            wait_time = (time - datetime.now()).total_seconds()
            Progress.sleep(wait_time=wait_time,
                           sleep_time=self.sleep_time,
                           info_text='Waiting for next task to process (%s seconds)... ' % round(wait_time, 3),
                           show_progressbar=self.show_progressbar and self.concurrency == 1)
            try:
                kwargs = celery_kwargs.get('kwargs')
                # remove additional metadata placed by Selinon when doing tracing
                kwargs.pop('meta', None)
                # Perform deep copy so any modification on task arguments does not affect arguments in queues.
                result = task.run(**copy.deepcopy(kwargs))

                # Dispatcher needs info about flow (JSON), but SelinonTaskEnvelope always returns None - we
                # need to keep track of success)
                SimulateAsyncResult.set_successful(task.request.id, result)
            except SimulateRetry as selinon_exc:
                if 'exc' in selinon_exc.celery_kwargs and selinon_exc.celery_kwargs.get('max_retries', 1) == 0:
                    # log only user exception as we do not want SimulateRetry in our exception traceback
                    user_exc = selinon_exc.celery_kwargs['exc']
                    user_exc_info = (user_exc, user_exc, user_exc.__traceback__)
                    self._logger.exception(str(user_exc), exc_info=user_exc_info)
                    SimulateAsyncResult.set_failed(task.request.id, traceback.format_exception(*user_exc_info))
                else:
                    # reschedule if there was an exception and we did not hit max_retries when doing retry
                    Executor.schedule(task, selinon_exc.celery_kwargs)
            except KeyboardInterrupt:  # pylint: disable=try-except-raise
                raise
            except Exception as exc:
                raise UnknownError("Ooooops! Congratulations! It looks like you've found a bug! Feel free to open an "
                                   "issue at https://github.com/selinon/selinon/issues") from exc

    @classmethod
    def schedule(cls, task, celery_kwargs):
        """Schedule a new task to be executed.

        :param task: task to be executed
        :type task: Dispatcher|SelinonTaskEnvelope
        :param celery_kwargs: arguments for the task - raw Celery arguments which also carry additional Selinon
                              arguments
        """
        cls._logger.debug("executor is scheduling %s - %s", task.__class__.__name__, celery_kwargs)
        cls.executor_queues.push(queue_name=celery_kwargs.get('queue', GlobalConfig.DEFAULT_CELERY_QUEUE),
                                 time=datetime.now() + timedelta(seconds=celery_kwargs.get('countdown') or 0),
                                 record=(task, celery_kwargs,))
