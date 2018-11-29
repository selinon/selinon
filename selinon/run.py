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

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def run_flow(flow_name, node_args=None, nodes_definition={}, flow_definitions={}):
    """Run flow by it's name.

    :param flow_name: name of the flow to be run
    :param node_args: arguments that will be supplied to flow
    :param nodes_definition: arguments that will be supplied to flow to update config
    :param flow_definitions: arguments that will be supplied to flow to update config
    :return: flow ID (dispatcher ID)
    """
    config_module = {}

    if Config.dispatcher_queues is None or flow_name not in Config.dispatcher_queues:
        raise UnknownFlowError("No flow with name '%s' defined" % flow_name)

    queue = Config.dispatcher_queues[flow_name]
    _logger.debug("Scheduling flow '%s' with node_args '%s' on queue '%s'", flow_name, node_args, queue)

    # construct config to pass into dispatcher
    config_module['dispatcher_queues'] = Config.dispatcher_queues
    config_module['task_queues'] = Config.task_queues
    config_module['nodes_definition'] = nodes_definition
    config_module['flow_definitions'] = flow_definitions

    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args,
                                            'config': config_module},
                                    queue=queue)


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
    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args,
                                            'selective': selective},
                                    queue=queue)
