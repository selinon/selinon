#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Errors and exceptions that can occur in Selinon code base."""

import json


class SelinonException(Exception):
    """Base Selinon exception in exception hierarchy."""


class FatalTaskError(SelinonException):
    """An exception that is raised by task on fatal error - task will be not retried."""


class UnknownFlowError(SelinonException):
    """Raised if there was requested or referenced flow that is not stated in the YAML configuration file."""


class UnknownStorageError(SelinonException):
    """Raised if there was requested or referenced storage that is not stated in the YAML configuration file."""


class ConfigurationError(SelinonException):
    """Raised on errors that indicate errors in the configuration files."""


class SelectiveNoPathError(SelinonException):
    """Raised when there is no path in the flow to requested node in selective task runs."""


class NoParentNodeError(SelinonException):
    """An exception raised when requested parent node (task/flow), but no such parent defined."""


class RequestError(SelinonException):
    """An error raised if there was an issue with request issued by user - usually means bad usage error."""


class UnknownError(SelinonException):
    """An error raised on unknown scenarios - possibly some bug in code."""


class Retry(SelinonException):
    """Retry task as would Celery do except you can only specify countdown for retry."""

    def __init__(self, countdown):
        """Init retry.

        :param countdown: countdown in seconds
        """
        self.countdown = countdown
        super().__init__(self, countdown)


class FlowError(SelinonException):
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


class DispatcherRetry(SelinonException):
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


class StorageError(SelinonException):
    """Raised to notify about storage errors (e.g. storage goes down)."""


class MigrationNotNeeded(SelinonException):
    """Raised when a migration is requested, but config changes do not require it."""


class MigrationSkew(SelinonException):
    """Raised if worker hasn't needed migration files."""

    def __init__(self, *args, available_migration_version):
        """Instantiate and track info about migration skew.

        :param available_migration_version:
        """
        self.available_migration_version = available_migration_version
        super().__init__(*args)


class MigrationException(SelinonException):
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


class CacheMissError(SelinonException):
    """An error raised when there is requested an item from cache that is not stored in cache."""


class ConfigNotInitializedError(SelinonException):
    """An error raised when the configuration was requested, but not initialized."""
