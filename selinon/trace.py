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
| SelinonTaskEnvelope                    |                         | parent, args, storage              |
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
| SelinonTaskEnvelope                    |                         | parent, args, what, retried_count  |
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
| this should not occur (e.g. bug,       |                         | state, node_args, parent, finished |
| database connection error, ...)        |                         | retry                              |
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

    DISPATCHER_WAKEUP, \
    FLOW_START, \
    TASK_SCHEDULE, \
    TASK_START, \
    SUBFLOW_SCHEDULE, \
    TASK_END, \
    NODE_SUCCESSFUL, \
    TASK_DISCARD_RESULT, \
    TASK_FAILURE, \
    TASK_RETRY, \
    FLOW_FAILURE, \
    DISPATCHER_FAILURE, \
    NODE_FAILURE, \
    FALLBACK_START, \
    DISPATCHER_RETRY, \
    FLOW_END, \
    STORAGE_CONNECT, \
    STORAGE_DISCONNECT, \
    STORAGE_RETRIEVE, \
    STORAGE_STORE = range(20)

    _event_strings = [
        'DISPATCHER_WAKEUP',
        'FLOW_START',
        'TASK_SCHEDULE',
        'TASK_START',
        'SUBFLOW_SCHEDULE',
        'TASK_END',
        'NODE_SUCCESSFUL',
        'TASK_DISCARD_RESULT',
        'TASK_FAILURE',
        'TASK_RETRY',
        'FLOW_FAILURE',
        'DISPATCHER_FAILURE',
        'NODE_FAILURE',
        'FALLBACK_START',
        'DISPATCHER_RETRY',
        'FLOW_END',
        'STORAGE_CONNECT',
        'STORAGE_DISCONNECT',
        'STORAGE_RETRIEVE',
        'STORAGE_STORE'
    ]

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
            logger = logging.getLogger('selinon_trace')
            formatter = logging.Formatter(prefix + ' - %(asctime)s.%(msecs)d %(levelname)s: %(message)s',
                                          datefmt="%Y-%m-%d %H:%M:%S")
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)
            logger.setLevel(level)
            # do not propagate to parent loggers so we don't get duplicate output
            logger.propagate = False

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

    @classmethod
    def event2str(cls, event):
        """
        Translate event to it's string representation

        :param event: event
        :return: string representation of event
        """
        # We could make a hack here in order to have O(1), but let's keep it this way
        return cls._event_strings[event]

    @classmethod
    def logging_trace_func(cls, event, msg_dict):
        """
        Trace to Python's logging facilities

        :param event: event that triggered trace point
        :param msg_dict: a dict holding additional trace information for event
        """
        message = "%s: %s" % (cls.event2str(event), msg_dict)

        if event == Trace.NODE_FAILURE or event == Trace.DISPATCHER_FAILURE or event == Trace.TASK_DISCARD_RESULT:
            return Trace._logger.warn(message)
        else:
            return Trace._logger.info(message)

