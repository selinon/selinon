#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""A task representation from YAML config file."""

import logging

from .errors import ConfigurationError
from .node import Node
from .selective_run_function import SelectiveRunFunction


class Task(Node):
    # pylint: disable=too-many-instance-attributes,arguments-differ
    """A task representation within the system."""

    _DEFAULT_MAX_RETRY = 0
    _DEFAULT_RETRY_COUNTDOWN = 0
    _logger = logging.getLogger(__name__)

    def __init__(self, name, import_path, storage, **opts):
        """Initialize a task node in a flow graph.

        :param name: name of the task
        :param import_path: tasks's import
        :param storage: storage that should be used
        :param opts: additional options for task
        """
        super().__init__(name)

        self.class_name = opts.pop('classname', name)
        self.storage = storage
        self.import_path = import_path

        if 'storage_task_name' in opts and not self.storage:
            raise ConfigurationError("Unable to assign storage_task_name for task '%s' (class '%s' from '%s'), task "
                                     "has no storage assigned" % (self.name, self.class_name, self.import_path))

        if 'selective_run_function' in opts:
            self.selective_run_function = SelectiveRunFunction.from_dict(opts.pop('selective_run_function'))
        else:
            self.selective_run_function = SelectiveRunFunction.get_default()

        self.storage_task_name = opts.pop('storage_task_name', name)
        self.output_schema = opts.pop('output_schema', None)

        if opts.get('retry_countdown') is not None and opts.get('max_retry', 0) == 0:
            self._logger.warning("Retry countdown set for task '%s' (class '%s' from '%s') but this task has"
                                 "retry set to 0", self.name, self.class_name, self.import_path)

        self.max_retry = opts.pop('max_retry', self._DEFAULT_MAX_RETRY)
        self.retry_countdown = opts.pop('retry_countdown', self._DEFAULT_RETRY_COUNTDOWN)

        self.queue_name = self._expand_queue_name(opts.pop('queue', None))
        self.storage_readonly = opts.pop('storage_readonly', False)
        self.throttling = self.parse_throttling(opts.pop('throttling', {}))

        if opts:
            raise ConfigurationError("Unknown task option provided for task '%s' (class '%s' from '%s'): %s"
                                     % (name, self.class_name, self.import_path, opts))

        # register task usage
        if self.storage:
            storage.register_task(self)

        self._logger.debug("Creating task with name '%s' import path '%s', class name '%s'",
                           self.name, self.import_path, self.class_name)

    def check(self):
        """Check task definitions for errors.

        :raises: ValueError if an error occurred
        """
        if not isinstance(self.import_path, str):
            raise ConfigurationError("Error in task '%s' definition - import path should be string; got '%s'"
                                     % (self.name, self.import_path))

        if self.class_name is not None and not isinstance(self.class_name, str):
            raise ConfigurationError("Error in task '%s' definition - class instance should be string; got '%s'"
                                     % (self.name, self.class_name))

        if self.output_schema is not None and not isinstance(self.output_schema, str):
            raise ConfigurationError("Error in task '%s' definition - output schema should be string; got '%s'"
                                     % (self.name, self.output_schema))

        if self.max_retry is not None and (not isinstance(self.max_retry, int) or self.max_retry < 0):
            raise ConfigurationError("Error in task '%s' definition - max_retry should be None, zero or positive "
                                     "integer; got '%s'" % (self.name, self.max_retry))

        if self.retry_countdown is not None and (not isinstance(self.retry_countdown, int) or self.retry_countdown < 0):
            raise ConfigurationError("Error in task '%s' definition - retry_countdown should be None or positive "
                                     "integer; got '%s'" % (self.name, self.retry_countdown))

        if self.queue_name is not None and not isinstance(self.queue_name, str):
            raise ConfigurationError("Invalid task queue, should be string, got %s" % self.queue_name)

        if not isinstance(self.storage_readonly, bool):
            raise ConfigurationError("Storage usage flag readonly should be of type bool")

    @staticmethod
    def from_dict(dictionary, system):
        """Construct task from a dict and check task's definition correctness.

        :param dictionary: dictionary to be used to construct the task
        :type dictionary: dict
        :param system: system that should be used to for lookup a storage
        :type system: System
        :return: Task instance
        :rtype: selinon.selinon_task.Task
        :raises: ValueError
        """
        if 'name' not in dictionary or not dictionary['name']:
            raise ConfigurationError('Task name definition is mandatory')
        if 'import' not in dictionary or not dictionary['import']:
            raise ConfigurationError('Task import definition is mandatory')
        if 'storage' in dictionary:
            storage = system.storage_by_name(dictionary.pop('storage'))
        else:
            storage = None

        instance = Task(dictionary.pop('name'), dictionary.pop('import'), storage, **dictionary)
        instance.check()
        return instance
