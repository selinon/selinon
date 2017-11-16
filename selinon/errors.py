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


class DispatcherRetry(Exception):
    """Force retry the whole flow - if flow arguments are provided, flow will continue where it was left."""

    def __init__(self, keep_state=True, adjust_retry_count=True):
        """Retry dispatcher causing flow state to be reinspected.

        :param keep_state: the current state should be kept otherwise the whole flow will start from the beginning
        :type keep_state: bool
        :param adjust_retry_count: adjust flow retry count if needed, will cause flow failure once retry count reaches 0
        :type adjust_retry_count: bool
        """
        super().__init__()
        self.keep_state = keep_state
        self.adjust_retry_count = adjust_retry_count


class StorageError(Exception):
    """Raised to notify about storage errors (e.g. storage goes down)."""


class CacheMissError(Exception):
    """An error raised when there is requested an item from cache that is not stored in cache."""


class ConfigNotInitializedError(Exception):
    """An error raised when the configuration was requested, but not initialized."""
