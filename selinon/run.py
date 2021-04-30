#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon flow scheduling (entrypoint) for a user."""

import logging

from .config import Config
from .dispatcher import Dispatcher
from .errors import UnknownFlowError
from .selective import compute_selective_run
from .trace import Trace

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def _do_run_flow(kwargs, queue):
    """Actually run the flow, return dispatcher id."""
    dispatcher_id = Dispatcher().apply_async(kwargs=kwargs, queue=queue)
    trace_msg = {
        'flow_name': kwargs["flow_name"],
        'node_args': kwargs["node_args"],
        'dispatcher_id': str(dispatcher_id),
        'queue': queue,
        'selective': kwargs.get("selective"),
    }
    Trace.log(Trace.FLOW_SCHEDULE, trace_msg)
    return dispatcher_id


def run_flow(flow_name, node_args=None):
    """Run flow by it's name.

    :param flow_name: name of the flow to be run
    :param node_args: arguments that will be supplied to flow
    :return: flow ID (dispatcher ID)
    """
    if Config.dispatcher_queues is None or flow_name not in Config.dispatcher_queues:
        raise UnknownFlowError("No flow with name '%s' defined" % flow_name)

    queue = Config.dispatcher_queues[flow_name]
    _logger.debug("Scheduling flow '%s' with node_args '%s' on queue '%s'", flow_name, node_args, queue)
    kwargs = {'flow_name': flow_name, 'node_args': node_args}
    return _do_run_flow(kwargs, queue)


def run_flow_selective(flow_name, task_names, node_args, follow_subflows=False, run_subsequent=False):
    """Run only desired tasks in a flow.

    :param flow_name: name of the flow that should be run
    :param task_names: name of the tasks that should be run
    :param node_args: arguments that should be supplied to flow
    :param follow_subflows: if True, subflows will be followed and checked for nodes to be run
    :param run_subsequent: trigger run of all tasks that depend on the desired task
    :return: dispatcher id that is scheduled to run desired selective task flow
    :raises selinon.errors.SelectiveNoPathError: there was no way found to the desired task in the flow
    """
    selective = compute_selective_run(flow_name, task_names, follow_subflows, run_subsequent)
    queue = Config.dispatcher_queues[flow_name]

    _logger.debug("Scheduling selective flow '%s' with node_args '%s' on queue '%s', computed selective run state: %s",
                  flow_name, node_args, queue, selective)
    kwargs = {'flow_name': flow_name, 'node_args': node_args, 'selective': selective}
    return _do_run_flow(kwargs, queue)
