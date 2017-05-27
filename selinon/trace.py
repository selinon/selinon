#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Built-in tracing mechanism.

List of events that can be traced:

+----------------------------------------+-------------------------+------------------------------------+
| Logged event                           |  Event name             | msg_dict.keys()                    |
+========================================+=========================+====================================+
| Signalize Dispatcher run by Celery,    |                         | flow_name, dispatcher_id, args,    |
| each time a Dispatcher is              | `DISPATCHER_WAKEUP`     | retry, state, queue                |
| started/retried                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a flow start - called when   |                         |                                    |
| starting nodes are run                 | `FLOW_START`            | flow_name, dispatcher_id, args,    |
|                                        |                         | queue                              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a task scheduling by         | `TASK_SCHEDULE`         | flow_name, task_name, task_id,     |
| Dispatcher                             |                         | parent, args, queue                |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task start by Celery         | `TASK_START`            | flow_name, task_name, task_id,     |
|                                        |                         | parent, args                       |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize subflow start by Dispatcher  | `SUBFLOW_SCHEDULE`      | flow_name, dispatcher_id,          |
|                                        |                         | child_flow_name, args, parent      |
|                                        |                         | child_dispatcher_id, queue         |
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
|                                        |                         | max_retry, retry_countdown,        |
|                                        |                         | user_retry                         |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize when a flow ends because of  | `FLOW_FAILURE`          | flow_name, dispatcher_id, what     |
| error in nodes without fallback        |                         | queue                              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize unexpected dispatcher failure| `DISPATCHER_FAILURE`    | flow_name, dispatcher_id, what     |
| this should not occur (e.g. bug,       |                         | state, node_args, parent, finished |
| database connection error, ...)        |                         | retry, queue                       |
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
|                                        |                         | state_dict, args, queue            |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize flow end                     | `FLOW_END`              | flow_name, dispatcher_id,          |
|                                        |                         | finished_nodes, queue              |
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
| Signalized when edge condition is      | `EDGE_COND_FALSE`       | nodes_to, nodes_from, flow_name,   |
| is evaluated as false                  |                         | parent, node_args, dispatcher_id   |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalized result of foreach  ing      | `FOREACH_RESULT`        | nodes_to, nodes_from, flow_name,   |
|                                        |                         | parent, node_args, dispatcher_id   |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+

"""

import datetime
import json
import logging
import platform


def _default_trace_func(event, msg_dict):
    # pylint: disable=unused-argument
    """Use default tracing function that is used for storing results - do nothing.

    :param event: event that triggered trace point
    :param msg_dict: a dict holding additional trace information for event
    """
    pass


class Trace(object):
    """Trace system flow actions."""

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
        STORAGE_RETRIEVED, \
        STORAGE_STORE, \
        STORAGE_STORED, \
        EDGE_COND_FALSE, \
        FOREACH_RESULT,\
        NODE_STATE_CACHE_GET,\
        NODE_STATE_CACHE_ADD,\
        NODE_STATE_CACHE_MISS,\
        NODE_STATE_CACHE_HIT,\
        TASK_RESULT_CACHE_GET,\
        TASK_RESULT_CACHE_ADD,\
        TASK_RESULT_CACHE_MISS,\
        TASK_RESULT_CACHE_HIT,\
        SELECTIVE_OMIT_EDGE,\
        SELECTIVE_OMIT_NODE,\
        SELECTIVE_RUN_FUNC,\
        SELECTIVE_TASK_REUSE,\
        STORAGE_STORE_ERROR,\
        STORAGE_OMIT_STORE_ERROR = range(38)

    WARN_EVENTS = (
        NODE_FAILURE,
        TASK_DISCARD_RESULT,
        TASK_RETRY,
        TASK_FAILURE,
        FLOW_FAILURE,
        STORAGE_OMIT_STORE_ERROR
    )

    _event_strings = (
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
        'STORAGE_RETRIEVED',
        'STORAGE_STORE',
        'STORAGE_STORED',
        'EDGE_COND_FALSE',
        'FOREACH_RESULT',
        'NODE_STATE_CACHE_GET',
        'NODE_STATE_CACHE_ADD',
        'NODE_STATE_CACHE_MISS',
        'NODE_STATE_CACHE_HIT',
        'TASK_RESULT_CACHE_GET',
        'TASK_RESULT_CACHE_ADD',
        'TASK_RESULT_CACHE_MISS',
        'TASK_RESULT_CACHE_HIT',
        'SELECTIVE_OMIT_EDGE',
        'SELECTIVE_OMIT_NODE',
        'SELECTIVE_RUN_FUNC',
        'SELECTIVE_TASK_REUSE',
        'STORAGE_STORE_ERROR',
        'STORAGE_OMIT_STORE_ERROR'
    )

    def __init__(self):
        """Unused."""
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls, logger=None):
        """Trace by using Python's logging.

        :param logger: optional logger that should be used
        """
        if not logger:
            logger = logging.getLogger(__name__)

        cls._logger = logger
        cls._trace_func = cls.logging_trace_func

    @classmethod
    def trace_by_func(cls, func):
        """Trace by a custom function.

        :param func: function with a one single argument
        """
        cls._trace_func = func

    @classmethod
    def log(cls, event, *msg_dict):
        """Log an event.

        :param event: tracing event
        :param msg_dict: message to be printed
        """
        to_report = {}
        for msg in msg_dict:
            to_report.update(msg)
        cls._trace_func(event, to_report)

    @classmethod
    def event2str(cls, event):
        """Translate event to it's string representation.

        :param event: event
        :return: string representation of event
        """
        return cls._event_strings[event]

    @classmethod
    def logging_trace_func(cls, event, msg_dict, logger=None):
        """Trace to Python's logging facilities.

        :param event: event that triggered trace point
        :param msg_dict: a dict holding additional trace information for event
        :param logger: a logger to be used, if None, logger from trace_by_logging will be used
        """
        report_dict = {
            'event': cls.event2str(event),
            'time': str(datetime.datetime.utcnow()),
            'details': msg_dict
        }
        message = "SELINON %10s - %s : %s" \
                  % (platform.node(), cls.event2str(event), json.dumps(report_dict, sort_keys=True))

        logger = logger or cls._logger
        if event == Trace.DISPATCHER_FAILURE:
            logger.error(message)
        elif event in cls.WARN_EVENTS:
            logger.warning(message)
        else:
            logger.info(message)
