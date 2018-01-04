#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


def my_custom_trace_func(event, msg_dict):
    """A custom trace function supplied by user."""
    print("Event: {}, msg_dict: {}".format(event, msg_dict))
