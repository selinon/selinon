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

import traceback
import jsonschema
from .storagePool import StoragePool
from .fatalTaskError import FatalTaskError
from celery import Task
from .trace import Trace
from .config import Config


class SelinonTaskEnvelope(Task):
    """
    A Celery task that is responsible for running user defined tasks from flow
    """
    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "SelinonTaskEnvelope"

    @classmethod
    def validate_result(cls, task_name, result):
        """
        :param task_name:
        :param result:
        :return:
        """
        schema_path = Config.output_schemas.get(task_name)
        if schema_path:
            with open(schema_path, "r") as f:
                schema = f.read()

            jsonschema.validate(result, schema)

    def run(self, task_name, flow_name, parent, node_args, finished=None, retried_count=None):
        """
        :param task_name:
        :param flow_name:
        :param parent:
        :param node_args:
        :param finished:
        :param retried_count:
        :return:
        """
        # we are passing args as one argument explicitly for now not to have troubles with *args and **kwargs mapping
        # since we depend on previous task and the result can be anything
        Trace.log(Trace.TASK_START, {'flow_name': flow_name,
                                     'task_name': task_name,
                                     'task_id': self.request.id,
                                     'parent': parent,
                                     'finished': finished,
                                     'args': node_args})
        try:
            task = Config.get_task_instance(task_name=task_name, flow_name=flow_name, parent=parent, finished=finished)
            result = task.run(node_args)
            self.validate_result(task_name, result)

            storage = StoragePool.get_storage_by_task_name(task_name)
            if storage and not Config.storage_readonly[task_name]:
                storage.store(node_args, flow_name, task_name, self.request.id, result)
            elif result is not None:
                Trace.log(Trace.TASK_DISCARD_RESULT, {'flow_name': flow_name,
                                                      'task_name': task_name,
                                                      'task_id': self.request.id,
                                                      'parent': parent,
                                                      'args': node_args,
                                                      'finished': finished,
                                                      'result': result})
        except Exception as exc:
            max_retry = Config.max_retry.get(task_name, 0)
            retried_count = 0 if retried_count is None else retried_count

            if max_retry > retried_count and not isinstance(exc, FatalTaskError):
                retried_count += 1
                retry_countdown = Config.retry_countdown.get(task_name, 0)

                kwargs = {
                    'task_name': task_name,
                    'flow_name': flow_name,
                    'parent': parent,
                    'node_args': node_args,
                    'retried_count': retried_count
                }

                Trace.log(Trace.TASK_RETRY, {'flow_name': flow_name,
                                             'task_name': task_name,
                                             'task_id': self.request.id,
                                             'parent': parent,
                                             'finished': finished,
                                             'args': node_args,
                                             'what': traceback.format_exc(),
                                             'retry_countdown': retry_countdown,
                                             'retried_count': retried_count,
                                             'max_retry': max_retry})
                raise self.retry(kwargs=kwargs, countdown=retry_countdown, queue=Config.task_queues[task_name])
            else:
                Trace.log(Trace.TASK_FAILURE, {'flow_name': flow_name,
                                               'task_name': task_name,
                                               'task_id': self.request.id,
                                               'parent': parent,
                                               'finished': finished,
                                               'args': node_args,
                                               'what': traceback.format_exc(),
                                               'retried_count': retried_count})
                raise self.retry(max_retries=0, exc=exc)

        Trace.log(Trace.TASK_END, {'flow_name': flow_name,
                                   'task_name': task_name,
                                   'task_id': self.request.id,
                                   'parent': parent,
                                   'finished': finished,
                                   'args': node_args,
                                   'storage': StoragePool.get_storage_name_by_task_name(task_name, graceful=True)})

