#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon utilities for a user."""

import logging

from selinonlib import UnknownFlowError

from .config import Config
from .dispatcher import Dispatcher
from .selective import compute_selective_run

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args},
                                    queue=queue)


def run_flow_selective(flow_name, task_names, node_args, follow_subflows=False, run_subsequent=False):
    """Run only desired tasks in a flow.

    :param flow_name: name of the flow that should be run
    :param task_names: name of the tasks that should be run
    :param node_args: arguments that should be supplied to flow
    :param follow_subflows: if True, subflows will be followed and checked for nodes to be run
    :param run_subsequent: trigger run of all tasks that depend on the desired task
    :return: dispatcher id that is scheduled to run desired selective task flow
    :raises selinonlib.errors.SelectiveNoPathError: there was no way found to the desired task in the flow
    """
    selective = compute_selective_run(flow_name, task_names, follow_subflows, run_subsequent)
    queue = Config.dispatcher_queues[flow_name]

    _logger.debug("Scheduling selective flow '%s' with node_args '%s' on queue '%s', computed selective run state: %s",
                  flow_name, node_args, queue, selective)
    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args,
                                            'selective': selective},
                                    queue=queue)
