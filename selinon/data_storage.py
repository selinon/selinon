#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Data storage interface."""

import abc


class SelinonMissingDataException(Exception):
    """Selinon exception that ressource was not found by storage"""
    pass

class DataStorage(metaclass=abc.ABCMeta):
    """Abstract Selinon storage adapter that is implemented by a user."""

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        """Initialize storage.

        :param args: storage arguments as stated in YAML configuration file
        :param kwargs: storage key-value arguments as stated in YAML configuration (preferred over args)
        """

    @abc.abstractmethod
    def connect(self):
        """Connect to a resource, if not needed, should be empty."""
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self):
        """Check storage connection status.

        :return: True if connected to a resource
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def disconnect(self):
        """Disconnect from a resource."""
        raise NotImplementedError()

    @abc.abstractmethod
    def retrieve(self, flow_name, task_name, task_id):
        """Retrieve result stored in storage.

        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be retrieved
        :param task_id: id of the task that result is going to be retrieved
        :return: task result
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store(self, node_args, flow_name, task_name, task_id, result):  # pylint: disable=too-many-arguments
        """Store result stored in storage.

        :param node_args: arguments that were passed to node
        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be stored
        :param task_id: id of the task that result is going to be stored
        :param result: result that should be stored
        :return: unique ID of stored record
        """
        raise NotImplementedError()

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # pylint: disable=too-many-arguments
        """Store information about task error.

        :param node_args: arguments that were passed to node
        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be stored
        :param task_id: id of the task that result is going to be stored
        :param exc_info: information about exception - tuple (type, value, traceback) as returned by sys.exc_info()
        :return: unique ID of stored record
        """
        # pylint: disable=abstract-method
        # no not mark this method with @abc.abstractmethod as we do not force a user to implement this
        raise NotImplementedError()

    def delete(self, flow_name, task_name, task_id):
        """Delete result stored in storage.

        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be retrieved
        :param task_id: id of the task that result is going to be retrieved
        """
        raise NotImplementedError("delete method is not implemented")

    def __del__(self):
        """Clean up."""
        if self.is_connected():
            self.disconnect()
