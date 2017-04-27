#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Errors that can be used or can occur outside Selinon."""


class FatalTaskError(Exception):
    """An exception that is raised by task on fatal error - task will be not retried."""

    pass


class InternalError(Exception):
    """Internal error of Selinon project, should not occur for end-user."""

    pass


class ConfigError(Exception):
    """Error raised when there is an error when parsing configuration files."""

    pass


class FlowError(Exception):
    """An exception that is raised once there is an error in the flow."""

    pass


class CacheMissError(Exception):
    """An error raised when there is requested an item from cache that is not stored in cache."""

    pass
