#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""A raw Celery task that is responsible for running SelinonTask."""

import json
import sys
import traceback

import jsonschema

from .celery import Task
from .config import Config
from .errors import FatalTaskError
from .errors import Retry
from .storage_pool import StoragePool
from .trace import Trace  # Ignore PyImportSortBear


class SelinonTaskEnvelope(Task):
    """A Celery task that is responsible for running user defined tasks from flow."""

    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "selinon.SelinonTaskEnvelope"

    @classmethod
    def validate_result(cls, task_name, result):
        """Validate result of the task for the given schema, if fails an Exception is raised.

        :param task_name: name of task
        :param result: result of task
        """
        schema_path = Config.output_schemas.get(task_name)
        if schema_path:
            with open(schema_path, "r") as input_file:
                schema = json.load(input_file)

            jsonschema.validate(result, schema)

    def selinon_retry(self, task_name, flow_name, parent, node_args, retry_countdown, retried_count,
                      dispatcher_id, user_retry=False):
        # pylint: disable=too-many-arguments
        """Retry on Celery level.

        :param task_name: name of the task to be retried
        :param flow_name: name of in which the task was run
        :param parent: dict of parent tasks
        :param node_args: arguments for task
        :param retry_countdown: countdown for retry
        :param retried_count: number of retries already done with this task
        :param dispatcher_id: ID id of dispatcher that is handling flow that run this task
        :param user_retry: True if retry was forced from the user
        """
        max_retry = Config.max_retry.get(task_name, 0)
        kwargs = {
            'task_name': task_name,
            'flow_name': flow_name,
            'parent': parent,
            'node_args': node_args,
            'dispatcher_id': dispatcher_id,
            'retried_count': retried_count
        }

        Trace.log(Trace.TASK_RETRY, {'flow_name': flow_name,
                                     'task_name': task_name,
                                     'task_id': self.request.id,
                                     'parent': parent,
                                     'node_args': node_args,
                                     'what': traceback.format_exc(),
                                     'retry_countdown': retry_countdown,
                                     'retried_count': retried_count,
                                     'user_retry': user_retry,
                                     'queue': Config.task_queues[task_name],
                                     'dispatcher_id': dispatcher_id,
                                     'max_retry': max_retry})
        raise self.retry(kwargs=kwargs, countdown=retry_countdown, queue=Config.task_queues[task_name])

    def run(self, task_name, flow_name, parent, node_args, dispatcher_id, retried_count=None):
        # pylint: disable=arguments-differ,too-many-arguments,too-many-locals
        """Task entry-point called by Celery.

        :param task_name: task to be run
        :param flow_name: flow in which this task run
        :param parent: dict of parent nodes
        :param node_args: node arguments within the flow
        :param dispatcher_id: dispatcher id that handles flow
        :param retried_count: number of already attempts that failed so task was retried
        :rtype: None
        """
        # we are passing args as one argument explicitly for now not to have troubles with *args and **kwargs mapping
        # since we depend on previous task and the result can be anything
        Trace.log(Trace.TASK_START, {'flow_name': flow_name,
                                     'task_name': task_name,
                                     'task_id': self.request.id,
                                     'parent': parent,
                                     'queue': Config.task_queues[task_name],
                                     'dispatcher_id': dispatcher_id,
                                     'node_args': node_args})
        try:
            task = Config.get_task_instance(
                task_name=task_name,
                flow_name=flow_name,
                parent=parent,
                task_id=self.request.id,
                dispatcher_id=dispatcher_id
            )
            result = task.run(node_args)
            self.validate_result(task_name, result)

            storage = StoragePool.get_storage_name_by_task_name(task_name, graceful=True)
            if storage and not Config.storage_readonly[task_name]:
                StoragePool.set(node_args, flow_name, task_name, self.request.id, result)
            elif result is not None:
                Trace.log(Trace.TASK_DISCARD_RESULT, {'flow_name': flow_name,
                                                      'task_name': task_name,
                                                      'task_id': self.request.id,
                                                      'parent': parent,
                                                      'node_args': node_args,
                                                      'queue': Config.task_queues[task_name],
                                                      'dispatcher_id': dispatcher_id,
                                                      'result': result})
        except Retry as retry:
            # we do not touch retried_count
            self.selinon_retry(task_name, flow_name, parent, node_args, retry.countdown, retried_count,
                               dispatcher_id, user_retry=True)
        except Exception as exc:  # pylint: disable=broad-except
            exc_info = sys.exc_info()
            max_retry = Config.max_retry.get(task_name, 0)
            retried_count = retried_count or 0

            if max_retry > retried_count and not isinstance(exc, FatalTaskError):
                retried_count += 1
                retry_countdown = Config.retry_countdown.get(task_name, 0)
                self.selinon_retry(task_name, flow_name, parent, node_args, retry_countdown, retried_count,
                                   dispatcher_id)
            else:
                Trace.log(Trace.TASK_FAILURE, {'flow_name': flow_name,
                                               'task_name': task_name,
                                               'task_id': self.request.id,
                                               'parent': parent,
                                               'node_args': node_args,
                                               'what': traceback.format_exc(),
                                               'queue': Config.task_queues[task_name],
                                               'dispatcher_id': dispatcher_id,
                                               'retried_count': retried_count})

                storage = StoragePool.get_storage_name_by_task_name(task_name, graceful=True)

                if storage and not Config.storage_readonly[task_name] \
                        and not StoragePool.set_error(node_args, flow_name, task_name, self.request.id, exc_info):
                    # TODO: move conversion to string to enhanced JSON handler and rather pass objects in Trace.log()
                    Trace.log(Trace.STORAGE_OMIT_STORE_ERROR, {
                        'flow_name': flow_name,
                        'node_args': node_args,
                        'task_name': task_name,
                        'task_id': self.request.id,
                        'error_type': str(exc_info[0]),
                        'error_value': str(exc_info[1]),
                        'error_traceback': "".join(traceback.format_tb(exc_info[2])),
                    })

                raise self.retry(max_retries=0, exc=exc)

        Trace.log(Trace.TASK_END, {'flow_name': flow_name,
                                   'task_name': task_name,
                                   'task_id': self.request.id,
                                   'parent': parent,
                                   'node_args': node_args,
                                   'queue': Config.task_queues[task_name],
                                   'dispatcher_id': dispatcher_id,
                                   'storage': StoragePool.get_storage_name_by_task_name(task_name, graceful=True)})
