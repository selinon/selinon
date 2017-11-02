#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Errors that can be used or can occur inside Selinon.

Exceptions explicitly stated and not imported from Selinonlib shouldn't be directly used by user. They are supposed
to be used in Selinon code tree.
"""

from selinonlib.errors import *  # pylint: disable=wildcard-import,unused-wildcard-import


class InternalError(Exception):
    """Internal error of Selinon project, should not occur for end-user."""


class FlowError(Exception):
    """An exception that is raised once there is an error in the flow on runtime - some nodes failed."""


class CacheMissError(Exception):
    """An error raised when there is requested an item from cache that is not stored in cache."""
