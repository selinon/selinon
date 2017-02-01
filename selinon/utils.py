#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""
Library utilities for a user
"""

from .config import Config
from .dispatcher import Dispatcher


def run_flow(flow_name, node_args=None):
    """
    Run flow by it's name

    :param flow_name: name of the flow to be run
    :param node_args: arguments that will be supplied to flow
    :return: flow ID (dispatcher ID)
    """
    if Config.dispatcher_queues is None or flow_name not in Config.dispatcher_queues:
        raise KeyError("No flow with name '%s' defined" % flow_name)

    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args},
                                    queue=Config.dispatcher_queues[flow_name])
