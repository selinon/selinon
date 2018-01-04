#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Abstract representation of nodes in task/flow dependencies - a node is either a task or a flow."""

import abc
import datetime
import os
import re

from .errors import ConfigurationError
from .global_config import GlobalConfig


class Node(metaclass=abc.ABCMeta):
    """An abstract class for node representation."""

    _NAME_RE = re.compile(r"^[_a-zA-Z][_a-zA-Z0-9]*$")

    def __init__(self, name):
        """Instantiate a node (flow/task)."""
        if not self.check_name(name):
            raise ConfigurationError("Invalid node name '%s'" % name)
        self._name = name

    @property
    def name(self):
        """Get name of the node.

        :return: a name of the node
        """
        return self._name

    def _expand_queue_name(self, queue_name):
        """Assign queue name based on configuration, do expansion based on env variables if needed.

        :param queue_name: name of queue as provided in config file or None
        :return: final name of the queue
        """
        if not queue_name:
            queue_name = GlobalConfig.default_dispatcher_queue if self.is_flow() else GlobalConfig.default_task_queue
        try:
            return queue_name.format(**os.environ)
        except KeyError as exc:
            if self.is_flow():
                err_msg = "Expansion of queue name based on environment variables failed for flow '%s', queue: '%s'" \
                          % (self.name, queue_name)
            else:
                err_msg = "Expansion of queue name based on environment variables failed for task '%s', queue: '%s' " \
                          % (self.name, queue_name)
            raise ConfigurationError(err_msg) from exc

    def is_flow(self):
        """Check if this node is a flow.

        :return: True if node represents a Flow
        """
        from .flow import Flow
        return isinstance(self, Flow)

    def is_task(self):
        """Check if this node is a task.

        :return: True if node represents a Task
        """
        from .task import Task
        return isinstance(self, Task)

    @classmethod
    def check_name(cls, name):
        """Check whether name is a correct node (flow/task) name.

        :param name: node name
        :return: True if name is a correct node name
        :rtype: bool
        """
        return cls._NAME_RE.match(name)

    def parse_throttling(self, dict_):
        """Parse throttling definition from a dictionary.

        :param dict_: dictionary from which throttling should be parsed
        :return: timedelta describing throttling countdown or None if not throttling applied
        :rtype: time.timedelta
        """
        if not dict_:
            return None

        if not isinstance(dict_, dict):
            raise ConfigurationError("Definition of throttling expects key value definition, got %s instead in '%s'"
                                     % (dict_['throttling'], self.name))
        try:
            return datetime.timedelta(**dict_)
        except TypeError as exc:
            raise ConfigurationError("Wrong throttling definition in '%s', expected values are %s"
                                     % (self.name, ['days', 'seconds', 'microseconds', 'milliseconds', 'minutes',
                                                    'hours', 'weeks'])) from exc
