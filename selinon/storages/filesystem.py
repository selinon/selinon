#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""A simple filesystem storage implementation."""

import json
import os

from selinon import DataStorage, SelinonMissingDataException


class Filesystem(DataStorage):
    """Selinon adapter for storing task results in a directory."""

    def __init__(self, path=None):
        """Instantiate Filesystem adapter.

        :param path: path to directory to be used
        :type path: str
        """
        super().__init__()
        self.path = (path or '{PWD}').format(**os.environ)
        self._connected = False

    def _construct_base_path(self, flow_name, task_name):
        return os.path.join(self.path, flow_name, task_name)

    def _construct_path(self, flow_name, task_name, task_id):
        return os.path.join(self._construct_base_path(flow_name, task_name), '{}.json'.format(task_id))

    def is_connected(self):
        return self._connected

    def connect(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        self._connected = True

    def disconnect(self):
        pass

    def retrieve(self, flow_name, task_name, task_id):
        path = self._construct_path(flow_name, task_name, task_id)
        with open(path, 'r') as result_file:
            return json.load(result_file)

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        base_path = self._construct_base_path(flow_name, task_name)
        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        path = self._construct_path(flow_name, task_name, task_id)
        with open(path, 'w') as result_file:
            json.dump(result, result_file)
        return path

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()

    def delete(self, flow_name, task_name, task_id):
        path = self._construct_path(flow_name, task_name, task_id)
        if os.path.exists(path):
            os.remove(path)
        else:
            raise SelinonMissingDataException
