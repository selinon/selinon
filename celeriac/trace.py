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
"""

List of events that can be traced:

+----------------------------------------+-------------------------+------------------------------------+
| Logged event                           |  Event name             | msg_dict.keys()                    |
+========================================+=========================+====================================+
| Signalize Dispatcher run by Celery,    |                         | flow_name, dispatcher_id, args,    |
| each time a Dispatcher is              | `DISPATCHER_WAKEUP`     | retry, state                       |
| started/retried                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a flow start - called when   |                         |                                    |
| starting nodes are run                 | `FLOW_START`            | flow_name, dispatcher_id, args     |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a task scheduling by         | `TASK_SCHEDULE`         | flow_name, task_name, task_id,     |
| Dispatcher                             |                         | parent, args                       |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task start by Celery         | `TASK_START`            | flow_name, task_name, task_id,     |
|                                        |                         | parent, args                       |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize subflow start by Dispatcher  | `SUBFLOW_SCHEDULE`      | flow_name, dispatcher_id,          |
|                                        |                         | child_flow_name, args, parent      |
|                                        |                         | child_dispatcher_id,               |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize end of task from             | `TASK_END`              | flow_name, task_name, task_id,     |
| CeleriacTaskEnvelope                   |                         | parent, args, storage              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a node success from          | `NODE_SUCCESSFUL`       | flow_name, dispatcher_id,          |
| Dispatcher                             |                         | node_name, node_id                 |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize that the result of task was  | `TASK_DISCARD_RESULT`   | flow_name, task_name, task_id,     |
| discarded                              |                         | parent, args, result               |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task failure from            | `TASK_FAILURE`          | flow_name, task_name, task_id,     |
| CeleriacTaskEnvelope                   |                         | parent, args, what, retried_count  |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task retry                   | `TASK_RETRY`            | flow_name, task_name, task_id,     |
|                                        |                         | parent, args, what, retried_count  |
|                                        |                         | max_retry, retry_countdown         |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize when a flow ends because of  | `FLOW_FAILURE`          | flow_name, dispatcher_id, what     |
| error in nodes without fallback        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize unexpected dispatcher failure| `DISPATCHER_FAILURE`    | flow_name, dispatcher_id, what     |
| this should not occur (e.g. bug,       |                         |                                    |
| database connection error, ...)        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a node failure from          | `NODE_FAILURE`          | flow_name, dispatcher_id,          |
| Dispatcher                             |                         | node_name, node_id, what           |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize fallback evaluation          | `FALLBACK_START`        | flow_name, dispatcher_id, nodes,   |
|                                        |                         | fallback                           |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize Dispatcher retry             | `DISPATCHER_RETRY`      | flow_name, dispatcher_id, retry,   |
|                                        |                         | state_dict, args                   |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize flow end                     | `FLOW_END`              | flow_name, dispatcher_id,          |
|                                        |                         | finished_nodes                     |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage connect                 | `STORAGE_CONNECT`       | storage_name                       |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage disconnect              | `STORAGE_DISCONNECT`    | storage_name                       |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage access for reading      | `STORAGE_RETRIEVE`      | flow_name, task_name, storage      |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage access for writing      | `STORAGE_STORE`         | flow_name, task_name, task_id,     |
|                                        |                         | storage, result, record_id         |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+

"""

import logging
import platform


def _default_trace_func(event, msg_dict):
    """
    Default tracing function that is used for storing results - do nothing

    :param event: event that triggered trace point
    :param msg_dict: a dict holding additional trace information for event
    """
    pass


class Trace(object):
    """
    Trace system flow actions
    """
    _trace_func = _default_trace_func
    _logger = None

    DISPATCHER_WAKEUP = 0
    FLOW_START = 1
    TASK_SCHEDULE = 2
    TASK_START = 3
    SUBFLOW_SCHEDULE = 4
    TASK_END = 5
    NODE_SUCCESSFUL = 6
    TASK_DISCARD_RESULT = 7
    TASK_FAILURE = 8
    TASK_RETRY = 9
    FLOW_FAILURE = 10
    DISPATCHER_FAILURE = 11
    NODE_FAILURE = 12
    FALLBACK_START = 13
    DISPATCHER_RETRY = 14
    FLOW_END = 15
    STORAGE_CONNECT = 16
    # TODO: currently unused
    STORAGE_DISCONNECT = 17
    STORAGE_RETRIEVE = 18
    STORAGE_STORE = 19

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls, level=logging.DEBUG, logger=None):
        """
        Trace by using Python's logging

        :param level: logging level
        :param logger: optional logger that should be used
        """
        prefix = 'DISPATCHER %10s' % platform.node()

        if not logger:
            logger = logging.getLogger('celeriac_trace')
            formatter = logging.Formatter(prefix + ' - %(asctime)s.%(msecs)d %(levelname)s: %(message)s',
                                          datefmt="%Y-%m-%d %H:%M:%S")
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)
            logger.setLevel(level)

        cls._logger = logger
        cls._trace_func = cls.logging_trace_func

    @classmethod
    def trace_by_func(cls, func):
        """
        Trace by a custom function

        :param func: function with a one single argument
        """
        cls._trace_func = func

    @classmethod
    def log(cls, event, msg_dict):
        """
        Trace work

        :param event: tracing event
        :param msg_dict: message to be printed
        """
        cls._trace_func(event, msg_dict)

    @staticmethod
    def logging_trace_func(event, msg_dict):
        """
        Trace to Python's logging facilities

        :param event: event that triggered trace point
        :param msg_dict: a dict holding additional trace information for event
        """
        logger = Trace._logger

        if event == Trace.DISPATCHER_WAKEUP:
            logger.info("Dispatcher woken up: %s" % msg_dict)
        elif event == Trace.FLOW_START:
            logger.info("Flow started: %s" % msg_dict)
        elif event == Trace.TASK_SCHEDULE:
            logger.info("Scheduled a new task: %s" % msg_dict)
        elif event == Trace.TASK_START:
            logger.info("Task has been started: %s" % msg_dict)
        elif event == Trace.SUBFLOW_SCHEDULE:
            logger.info("Scheduled a new subflow: %s" % msg_dict)
        elif event == Trace.TASK_END:
            logger.info("Task has successfully finished: %s" % msg_dict)
        elif event == Trace.NODE_SUCCESSFUL:
            logger.info("Node in flow successfully finished: %s" % msg_dict)
        elif event == Trace.TASK_DISCARD_RESULT:
            logger.warning("Result of task has been discarded: %s" % msg_dict)
        elif event == Trace.TASK_FAILURE:
            logger.warning("Task has failed: %s" % msg_dict)
        elif event == Trace.TASK_RETRY:
            logger.warning("Task will be retried: %s" % msg_dict)
        elif event == Trace.FLOW_FAILURE:
            logger.warning("Flow has failed: %s" % msg_dict)
        elif event == Trace.DISPATCHER_FAILURE:
            logger.error("Dispatcher failed: %s" % msg_dict)
        elif event == Trace.NODE_FAILURE:
            logger.warning("Node has failed: %s" % msg_dict)
        elif event == Trace.FALLBACK_START:
            logger.info("Fallback is going to be started started: %s" % msg_dict)
        elif event == Trace.DISPATCHER_RETRY:
            logger.info("Dispatcher will be rescheduled: %s" % msg_dict)
        elif event == Trace.FLOW_END:
            logger.info("Flow has successfully ended: %s" % msg_dict)
        elif event == Trace.STORAGE_CONNECT:
            logger.info("Requested connect to storage: %s" % msg_dict)
        elif event == Trace.STORAGE_DISCONNECT:
            logger.info("Requested disconnect from storage: %s" % msg_dict)
        elif event == Trace.STORAGE_RETRIEVE:
            logger.info("Retrieving data from storage: %s" % msg_dict)
        elif event == Trace.STORAGE_STORE:
            logger.info("Storing data in storage: %s" % msg_dict)
        else:
            logger.info("Unhandled event (%s): %s" % (event, msg_dict))

