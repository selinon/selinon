#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


def strategy_function(status):
    """
    Strategy function for system state sampling - simplified version, more robust solutions tested in selinon
    """
    if len(status['active_nodes']) > 0 or len(status['new_started_nodes']) > 0:
        return 2
    else:
        return None
