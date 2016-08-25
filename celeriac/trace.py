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
    logging.info("" % msg_dict)


class Trace(object):
    """
    Trace Dispatcher work
    """
    _trace_func = _default_trace_func

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
    SUBFLOW_SCHEDULE = 4                          # flow_name, dispatcher_id, child_flow_name, child_dispatcher_id, args

    # Signalize end of task from CeleriacTask
    TASK_END = 5                                  # flow_name, task_name, task_id, parent, args, storage

    # Signalize a node success from dispatcher
    NODE_SUCCESSFUL = 6                           # flow_name, dispatcher_id, node_name, node_id

    # Signalize that the result of task was discarded
    TASK_DISCARD_RESULT = 7                       # flow_name, task_name, task_id, parent, args, result

    # Signalize task failure from CeleriacTask
    TASK_FAILURE = 8                              # flow_name, task_name, task_id, parent, args, what

    # Signalize when a flow ends because of error in nodes without fallback
    FLOW_FAILURE = 9                              # flow_name, dispatcher_id, what

    # Signalize unexpected dispatcher failure - this should not occur (e.g. bug, database connection error, ...)
    DISPATCHER_FAILURE = 10                       # flow_name, dispatcher_id, what

    # Signalize a node failure from dispatcher
    NODE_FAILURE = 11                             # flow_name, dispatcher_id, node_name, node_id, what

    # Signalize fallback evaluation
    FALLBACK_START = 12                           # flow_name, dispatcher_id, nodes, fallback

    # Signalize Dispatcher retry
    DISPATCHER_RETRY = 13                         # flow_name, dispatcher_id, retry, state_dict, args

    # Signalize flow end
    FLOW_END = 14                                 # flow_name, dispatcher_id, finished_nodes

    # Signal storage connect
    STORAGE_CONNECT = 15                          # storage_name

    # Signal storage disconnect
    # TODO: currently unused
    STORAGE_DISCONNECT = 16                       # storage_name

    # Signal storage access for reading
    STORAGE_RETRIEVE = 17                         # flow_name, task_name, storage

    # Signal storage access for writing
    STORAGE_STORE = 18                            # flow_name, task_name, task_id, storage, result

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls):
        """
        Trace by using Python's logging
        """
        prefix = 'DISPATCHER %10s' % platform.node()
        logging.basicConfig(level=logging.INFO,
                            format=prefix + ' - %(asctime)s.%(msecs)d %(levelname)s: %(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S")
        cls._trace_func = _logging_trace_func

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


def _logging_trace_func(event, msg_dict):
    if event == Trace.DISPATCHER_WAKEUP:
        logging.info("Dispatcher woken up: %s" % msg_dict)
    elif event == Trace.FLOW_START:
        logging.info("Flow started: %s" % msg_dict)
    elif event == Trace.TASK_SCHEDULE:
        logging.info("Scheduled a new task: %s" % msg_dict)
    elif event == Trace.TASK_START:
        logging.info("Task has been started: %s" % msg_dict)
    elif event == Trace.SUBFLOW_SCHEDULE:
        logging.info("Scheduled a new subflow: %s" % msg_dict)
    elif event == Trace.TASK_END:
        logging.info("Task has successfully finished: %s" % msg_dict)
    elif event == Trace.NODE_SUCCESSFUL:
        logging.info("Node in flow successfully finished: %s" % msg_dict)
    elif event == Trace.TASK_DISCARD_RESULT:
        logging.warning("Result of task has been discarded: %s" % msg_dict)
    elif event == Trace.TASK_FAILURE:
        logging.info("Task has failed: %s" % msg_dict)
    elif event == Trace.FLOW_FAILURE:
        logging.info("Flow has failed: %s" % msg_dict)
    elif event == Trace.DISPATCHER_FAILURE:
        logging.info("Dispatcher failed: %s" % msg_dict)
    elif event == Trace.NODE_FAILURE:
        logging.info("Node has failed: %s" % msg_dict)
    elif event == Trace.FALLBACK_START:
        logging.info("Fallback is going to be started started: %s" % msg_dict)
    elif event == Trace.DISPATCHER_RETRY:
        logging.info("Dispatcher will be rescheduled: %s" % msg_dict)
    elif event == Trace.FLOW_END:
        logging.info("Flow has successfully ended: %s" % msg_dict)

