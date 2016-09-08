#!/usr/bin/env python
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

import logging
import platform


def _default_trace_func(event, msg_dict):
    pass


class Trace(object):
    """
    Trace system flow actions
    """
    _trace_func = _default_trace_func
    _logger = None

    # TODO: make this nice for Sphinx (?)
    # Logged event                                # msg_dict.keys()
    ################################################################################
    # Signalize Dispatcher run by Celery - each time a Dispatcher is started/retried
    DISPATCHER_WAKEUP = 0                         # flow_name, dispatcher_id, args, retry, state

    # Signalize a flow start - called when starting nodes are run
    FLOW_START = 1                                # flow_name, dispatcher_id, args

    # Signalize a task scheduling by Dispatcher
    TASK_SCHEDULE = 2                             # flow_name, task_name, task_id, parent, args

    # Signalize task start by Celery
    TASK_START = 3                                # flow_name, task_name, task_id, parent, args

    # Signalize subflow start by Dispatcher
    SUBFLOW_SCHEDULE = 4                          # flow_name, dispatcher_id, child_flow_name, child_dispatcher_id,
                                                  # args, parent

    # Signalize end of task from CeleriacTask
    TASK_END = 5                                  # flow_name, task_name, task_id, parent, args, storage

    # Signalize a node success from dispatcher
    NODE_SUCCESSFUL = 6                           # flow_name, dispatcher_id, node_name, node_id

    # Signalize that the result of task was discarded
    TASK_DISCARD_RESULT = 7                       # flow_name, task_name, task_id, parent, args, result

    # Signalize task failure from CeleriacTask
    TASK_FAILURE = 8                              # flow_name, task_name, task_id, parent, args, what, retried_count

    # Signalize task retry
    TASK_RETRY = 9                                # flow_name, task_name, task_id, parent, args, what, retried_count,
                                                  # max_retry, retry_countdown

    # Signalize when a flow ends because of error in nodes without fallback
    FLOW_FAILURE = 10                             # flow_name, dispatcher_id, what

    # Signalize unexpected dispatcher failure - this should not occur (e.g. bug, database connection error, ...)
    DISPATCHER_FAILURE = 11                       # flow_name, dispatcher_id, what

    # Signalize a node failure from dispatcher
    NODE_FAILURE = 12                             # flow_name, dispatcher_id, node_name, node_id, what

    # Signalize fallback evaluation
    FALLBACK_START = 13                           # flow_name, dispatcher_id, nodes, fallback

    # Signalize Dispatcher retry
    DISPATCHER_RETRY = 14                         # flow_name, dispatcher_id, retry, state_dict, args

    # Signalize flow end
    FLOW_END = 15                                 # flow_name, dispatcher_id, finished_nodes

    # Signal storage connect
    STORAGE_CONNECT = 16                          # storage_name

    # Signal storage disconnect
    # TODO: currently unused
    STORAGE_DISCONNECT = 17                       # storage_name

    # Signal storage access for reading
    STORAGE_RETRIEVE = 18                         # flow_name, task_name, storage

    # Signal storage access for writing
    STORAGE_STORE = 19                            # flow_name, task_name, task_id, storage, result

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls, level=logging.DEBUG):
        """
        Trace by using Python's logging
        :param level: logging level
        """
        prefix = 'DISPATCHER %10s' % platform.node()

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

