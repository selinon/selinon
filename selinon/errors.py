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

import json

from selinonlib.errors import *  # pylint: disable=wildcard-import,unused-wildcard-import


class FlowError(Exception):
    """An exception that is raised once there is an error in the flow on runtime - some nodes failed."""

    def __init__(self, state):
        """Make sure flow errors capture final state of the flow.

        :param state: final flow state
        """
        super().__init__(json.dumps(state))

    @property
    def state(self):
        """Get structured flow state."""
        return json.loads(str(self))


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


class MigrationNotNeeded(Exception):
    """Raised when a migration is requested, but config changes do not require it."""


class MigrationSkew(Exception):
    """Raised if worker hasn't needed migration files."""

    def __init__(self, *args, available_migration_version):
        """Instantiate and track info about migration skew.

        :param available_migration_version:
        """
        self.available_migration_version = available_migration_version
        super().__init__(*args)


class MigrationException(Exception):
    """Base exception for migration related exceptions."""

    TAINTED_FLOW_STRATEGY = 'UNKNOWN'

    def __init__(self, *args, migration_version, latest_migration_version, tainting_nodes=None, tainted_edge=None):
        """Instantiate base exception for migrations.

        :param args: additional arguments for base exception
        :param migration_version: the current migration version
        :param latest_migration_version: the latest migration version based on migration directory content
        :param tainting_nodes: nodes that run tainted flow
        :param tainted_edge: edges that tainted flow
        """
        self.migration_version = migration_version
        self.latest_migration_version = latest_migration_version
        self.tainting_nodes = tainting_nodes
        self.tainted_edge = tainted_edge
        super().__init__(*args)


class MigrationFlowFail(MigrationException):
    """An exception raised when a flow should fail - i.e. migration causing tainting of flow should fail flow."""

    TAINTED_FLOW_STRATEGY = 'FAIL'


class MigrationFlowRetry(MigrationException):
    """An exception raised when a flow should be retried - i.e. migration causing tainting of flow should retry flow."""

    TAINTED_FLOW_STRATEGY = 'RETRY'


class CacheMissError(Exception):
    """An error raised when there is requested an item from cache that is not stored in cache."""


class ConfigNotInitializedError(Exception):
    """An error raised when the configuration was requested, but not initialized."""
