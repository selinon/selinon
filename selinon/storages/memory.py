#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""In memory storage implementation."""

import json as jsonlib
from selinon.data_storage import SelinonMissingDataException
import sys

from selinon import DataStorage


class InMemoryStorage(DataStorage):
    """Storage that stores results in memory without persistence."""

    def __init__(self, echo=False, json=False):
        """Initialize storage, values passed from YAML cofig file.

        :param echo: echo results to stdout/stderr - provide 'stderr' or 'stdout' to echo data retrieval and storing
        :param json: if True a JSON will be printed
        """
        super().__init__()
        self.database = {}
        self.echo_file = None

        if not echo and json:
            raise ValueError("JSON parameter requires echo to be specified ('stdout' or 'stderr')")

        self.echo_json = json

        if echo == 'stdout' or echo is True:
            self.echo_file = sys.stdout
        elif echo == 'stderr':
            self.echo_file = sys.stderr

    def is_connected(self):  # noqa
        return True

    def connect(self):  # noqa
        pass

    def disconnect(self):  # noqa
        pass

    def retrieve(self, flow_name, task_name, task_id):  # noqa
        try:
            result = self.database[task_id]['result']
            if self.echo_file and self.echo_json:
                jsonlib.dump(result, self.echo_file, sort_keys=True, separators=(',', ': '), indent=2)
            elif self.echo_file:
                print(result, file=self.echo_file)
        except KeyError:
            raise FileNotFoundError("Record not found in database")

        return result

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert task_id not in self.database  # nosec

        record = {
            'node_args': node_args,
            'task_name': task_name,
            'flow_name': flow_name,
            'task_id': task_id,
            'result': result
        }
        self.database[task_id] = record

        if self.echo_file and self.echo_json:
            jsonlib.dump(record, self.echo_file, sort_keys=True, separators=(',', ': '), indent=2)
        elif self.echo_file:
            print(record, file=self.echo_file)

        # task_id is unique for the record
        return task_id

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()

    def delete(self, flow_name, task_name, task_id):
        try:
            del self.database[task_id]
        except KeyError:
            raise SelinonMissingDataException("Record not found in database")
