#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
# pylint: disable=line-too-long
"""Built-in tracing mechanism.

A list of events that can be traced:


+----------------------------+-------------------------------------+-----------------+------------------------------------+
| Event                      |  Event description                  | Emitter         |  msg_dict.keys()                   |
+============================+=====================================+=================+====================================+
|                            | A flow has been scheduled.          |                 |  dispatcher_id, selective,         |
|   `FLOW_SCHEDULE`          |                                     | Msg producer    |  queue, node_args,                 |
|                            |                                     |                 |  flow_name                         |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Dispatcher was started and will     |                 |  dispatcher_id, state, selective,  |
|   `DISPATCHER_WAKEUP`      | check flow status.                  | Dispatcher      |  retry, queue, node_args,          |
|                            |                                     |                 |  flow_name                         |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | A new flow is starting, this event  |                 |  dispatcher_id, selective,         |
|   `FLOW_START`             | is emitted on after                 | Dispatcher      |  queue, node_args, flow_name       |
|                            | `DISPATCHER_WAKEUP`.                |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Emitted when a new task is          |                 |  countdown, condition_str,         |
|   `TASK_SCHEDULE`          | scheduled by dispatcher.            | Dispatcher      |  task_name, queue,                 |
|                            |                                     |                 |  node_args, dispatcher_id, parent, |
|                            |                                     |                 |  task_id, selective, foreach_str,  |
|                            |                                     |                 |  flow_name, selective_edge         |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Emitted when a task is going to     |                 |                                    |
|   `TASK_START`             | be executed on worker side.         | Task            |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Emitted when a flow is scheduled    |                 |                                    |
|   `SUBFLOW_SCHEDULE`       | by dispatcher.                      | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizing end of task execution,  |                 |                                    |
|   `TASK_END`               | task finished successfully.         | Task            |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizing that a node in flow     |                 |                                    |
|   `NODE_SUCCESSFUL`        | graph (task or flow) successfully   | Dispatcher      |                                    |
|                            | finished.                           |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizing that a task returned a  |                 |                                    |
|   `TASK_DISCARD_RESULT`    | value other than None, but no       | Task            |                                    |
|                            | storage was assigned to task to     |                 |                                    |
|                            | store result.                       |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizing end of task, task       |                 |                                    |
|   `TASK_FAILURE`           | raised an exception, marking task   | Task            |                                    |
|                            | as failed.                          |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizing that a task failed by,  |                 |                                    |
|   `TASK_RETRY`             | raising an exception and will be    | Task            |                                    |
|                            | retried.                            |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalization of flow failure,      |                 |                                    |
|   `FLOW_FAILURE`           | one more nodes in flow graph failed | Dispatcher      |                                    |
|                            | without successful fallback run.    |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | This event shouldn't be normally    |                 |                                    |
|   `DISPATCHER_FAILURE`     | seen - signalizing error in         | Dispatcher      |                                    |
|                            | Selinon.                            |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalization of captured failure   |                 |                                    |
|   `NODE_FAILURE`           | of node in task flow graph - flow   | Dispatcher      |                                    |
|                            | or task failure.                    |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Fallback for node failures in flow  |                 |                                    |
|   `FALLBACK_START`         | is started to handle node failures. | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Dispatcher finished scheduling new  |                 | dispatcher_id, state, selective,   |
|   `DISPATCHER_RETRY`       | nodes and will retry to check flow  | Dispatcher      | retry, queue, node_args,           |
|                            | status after a while.               |                 | state_dict, flow_name, parent      |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Flow has successfully ended.        |                 |                                    |
|   `FLOW_END`               |                                     | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Flow has migration was done.        |                 |                                    |
|   `MIGRATION`              |                                     | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalization of tainted flow when  |                 |                                    |
|   `MIGRATION_TAINTED_FLOW` | migration was run.                  | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Flow migration has error - skewed   |                 |                                    |
|   `MIGRATION_SKEW`         | migration.                          | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Given node did not connect to       |                 | storage_name                       |
|   `STORAGE_CONNECT`        | storage yet so a new connection is  | Dispatcher/Task |                                    |
|                            | requested by calling                |                 |                                    |
|                            | DataStorage.connect()               |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Called when disconnecting from      |                 |                                    |
|   `STORAGE_DISCONNECT`     | storage by calling                  | storage adapter |                                    |
|                            | DataStorage.disconnect()            | destructor      |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Retrieve task result from storage   |                 |                                    |
|   `STORAGE_RETRIEVE`       | that was assigned to the task.      | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested result of task was        |                 |                                    |
|   `STORAGE_RETRIEVED`      | retrieved.                          | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Store result of task in the         |                 |                                    |
|   `STORAGE_STORE`          | assigned storage.                   | Task            |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | The result of task has been stored  |                 |                                    |
|   `STORAGE_STORED`         | in the assigned storage.            | Task            |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | The condition on edge was evaluated |                 |                                    |
|   `EDGE_COND_FALSE`        | as false so destination nodes will  | Dispatcher      |                                    |
|                            | not be scheduled.                   |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Reports result of foreach function, |                 |                                    |
|   `FOREACH_RESULT`         | based on which N nodes will be      | Dispatcher      |                                    |
|                            | scheduled (N is runtime variable)   |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested state of the node (if the |                 |                                    |
|   `NODE_STATE_CACHE_GET`   | node was successful or failed) from | Dispatcher      |                                    |
|                            | cache.                              |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested entry in the state cache  |                 |                                    |
|   `NODE_STATE_CACHE_ADD`   | was not found and the node          | Dispatcher      |                                    |
|                            | succeeded so new entry to state     |                 |                                    |
|                            | is added.                           |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested entry was not found in    |                 |                                    |
|   `NODE_STATE_CACHE_MISS`  | state cache.                        | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested entry was found in the    |                 |                                    |
|   `NODE_STATE_CACHE_HIT`   | state cache and will be used.       | Dispatcher      |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Try to hit result cache for cached  |                 |                                    |
|   `TASK_RESULT_CACHE_GET`  | already requested task result.      | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Add to result cache result of a     |                 |                                    |
|   `TASK_RESULT_CACHE_ADD`  | task that was requested.            | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested task result was not found |                 |                                    |
|   `TASK_RESULT_CACHE_MISS` | in the result cache.                | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Requested task result was found     |                 |                                    |
|   `TASK_RESULT_CACHE_HIT`  | in the result cache.                | Dispatcher/Task |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Given edge will not be fired        |                 |                                    |
|   `SELECTIVE_OMIT_EDGE`    | as it is not part of direct path    | Dispatcher      |                                    |
|                            | to requested tasks on selective run.|                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | The desired node will not be        |                 |                                    |
|   `SELECTIVE_OMIT_NODE`    | scheduled as it is not requested    | Dispatcher      |                                    |
|                            | task nor dependency of requested    |                 |                                    |
|                            | task in selective task runs.        |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Keeps track of results of selective |                 | dispatcher_id, parent, selective,  |
|   `SELECTIVE_RUN_FUNC`     | run function on selective task      | Dispatcher      | node_args, flow_name,              |
|                            | runs.                               |                 | result, node_name                  |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizes reuse of already         |                 |                                    |
|   `SELECTIVE_TASK_REUSE`   | computed results in selective task  | Dispatcher      |                                    |
|                            | runs.                               |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizes storing error information|                 |                                    |
|   `STORAGE_STORE_ERROR`    | in assigned storage on task failure.| Task            |                                    |
|                            |                                     |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+
|                            | Signalizes that storing error       |                 |                                    |
| `STORAGE_OMIT_STORE_ERROR` | will not be done - missing storage  | Task            |                                    |
|                            | adapter or `store_error()` is not   |                 |                                    |
|                            | implemented.                        |                 |                                    |
+----------------------------+-------------------------------------+-----------------+------------------------------------+

"""

import datetime
import json
import logging
import os
import platform
import sys

_LOGGER = logging.getLogger(__name__)


class Trace:
    """Trace system flow actions."""

    _trace_functions = []
    _logger = None

    DISPATCHER_WAKEUP, \
        FLOW_SCHEDULE, \
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
        STORAGE_OMIT_STORE_ERROR,\
        FALLBACK_COND_FALSE,\
        FALLBACK_COND_TRUE,\
        NODE_STATE_CACHE_ISSUE,\
        TASK_RESULT_CACHE_ISSUE,\
        STORAGE_ISSUE,\
        RESULT_BACKEND_ISSUE,\
        FLOW_RETRY,\
        MIGRATION,\
        MIGRATION_SKEW,\
        MIGRATION_TAINTED_FLOW, \
        MIGRATION_ERROR, \
        EAGER_FAILURE,\
        STORAGE_DELETE, \
        STORAGE_DELETED, \
        = range(53)

    WARN_EVENTS = (
        NODE_FAILURE,
        TASK_DISCARD_RESULT,
        TASK_RETRY,
        TASK_FAILURE,
        FLOW_FAILURE,
        STORAGE_OMIT_STORE_ERROR,
        NODE_STATE_CACHE_ISSUE,
        TASK_RESULT_CACHE_ISSUE,
        STORAGE_ISSUE,
        RESULT_BACKEND_ISSUE,
        FLOW_RETRY,
        MIGRATION_SKEW,
        MIGRATION_ERROR,
        EAGER_FAILURE,
    )

    _event_strings = (
        'DISPATCHER_WAKEUP',
        'FLOW_SCHEDULE',
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
        'STORAGE_OMIT_STORE_ERROR',
        'FALLBACK_COND_FALSE',
        'FALLBACK_COND_TRUE',
        'NODE_STATE_CACHE_ISSUE',
        'TASK_RESULT_CACHE_ISSUE',
        'STORAGE_ISSUE',
        'RESULT_BACKEND_ISSUE',
        'FLOW_RETRY',
        'MIGRATION',
        'MIGRATION_SKEW',
        'MIGRATION_TAINTED_FLOW',
        'MIGRATION_ERROR',
        'EAGER_FAILURE',
        'STORAGE_DELETE',
        'STORAGE_DELETED'
    )

    def __init__(self):
        """Unused."""
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls, logger=None):
        """Trace by using Python's logging.

        :param logger: optional logger that should be used
        """
        if not logger and not cls._logger:
            logger = logging.getLogger(__name__)

        cls._logger = logger
        cls._trace_functions.append(cls.logging_trace_func)

    @classmethod
    def trace_by_json(cls):
        """Trace by writing directly JSON trace points."""
        cls._trace_functions.append(cls.json_trace_func)

    @classmethod
    def trace_by_sentry(cls, dsn):
        """Trace using Sentry (see https://sentry.io).

        :param dsn: data source name for connecting to Sentry
        """
        try:
            import sentry_sdk
        except ImportError as exc:
            raise ImportError("Failed to import Sentry-SDK for Sentry logging, install it using `pip3 install sentry-sdk`")\
                from exc

        sentry_sdk.init(dsn.format(**os.environ))

    @classmethod
    def trace_by_func(cls, func):
        """Trace by a custom function.

        :param func: function with a one single argument
        """
        cls._trace_functions.append(func)

    @classmethod
    def log(cls, event, *msg_dict, **msg_dict_kwargs):
        """Log an event.

        :param event: tracing event
        :param msg_dict: message to be printed
        :param msg_dict_kwargs: kwargs like dictionary for traced details
        """
        if not cls._trace_functions:
            return

        to_report = {}
        for msg in msg_dict:
            to_report.update(msg)

        to_report.update(msg_dict_kwargs)

        for trace_func in cls._trace_functions:
            try:
                trace_func(event, to_report)
            except Exception as exc:  # pylint: disable=broad-except
                # To prevent from recursively reporting errors, this error is reported only to standard logging.
                _LOGGER.exception("Failed to report to trace function %r: %s", trace_func, str(exc))

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

    @classmethod
    def json_trace_func(cls, event, msg_dict):
        """Trace by directly printing JSONs to stdout or stderr.

        :param event: event that triggered trace point
        :param msg_dict: a dict holding additional trace information for event
        """
        report_dict = {
            'event': cls.event2str(event),
            'time': str(datetime.datetime.utcnow()),
            'details': msg_dict,
            'node': platform.node()
        }
        # We could use simplejson here so this will be faster
        if event in cls.WARN_EVENTS:
            print(json.dumps(report_dict, sort_keys=True), file=sys.stderr)
        else:
            print(json.dumps(report_dict, sort_keys=True))
