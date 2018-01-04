#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Supporting routines for run time."""


def always_run(flow_name, node_name, node_args, task_names, storage_pool):  # pylint: disable=unused-argument
    """Run the default function that is called on selective run.

    :param flow_name: flow name in which the selective run is done
    :param node_name: name of the node on which this function was run
    :param node_args: arguments supplied to the flow
    :param task_names: a list of nodes that should be run in the selective run
    :param storage_pool: storage pool for the parent nodes
    :return: None if desired node should be run, id of task that results should be reused
    """
    return None
