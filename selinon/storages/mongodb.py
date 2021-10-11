#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""MongoDB database adapter."""

import os
from selinon.data_storage import SelinonMissingDataException

try:
    from pymongo import MongoClient
except ImportError as exc:
    raise ImportError("Please install dependencies using `pip3 install selinon[mongodb]` "
                      "in order to use MongoStorage") from exc
from selinon import DataStorage


class MongoDB(DataStorage):
    """MongoDB database adapter."""

    def __init__(self, db_name, collection_name, host=None, port=27017):
        """Instantiate MongoDB storage adapter.

        :param db_name: MongoDB database name
        :param collection_name: MongoDB collection name
        :param host: MongoDB host
        :param port: MongoDB port
        """
        super().__init__()
        self.client = None
        self.collection = None
        self.db = None  # pylint: disable=invalid-name
        self.host = (host or 'localhost').format(**os.environ)
        self.port = int(port.format(**os.environ) if isinstance(port, str) else port)
        self.db_name = db_name.format(**os.environ)
        self.collection_name = collection_name.format(**os.environ)

    def is_connected(self):  # noqa
        return self.client is not None

    def connect(self):  # noqa
        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def disconnect(self):  # noqa
        if self.is_connected():
            self.client.close()
            self.client = None
            self.db = None
            self.collection = None

    def retrieve(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        filtering = {'_id': 0}
        cursor = self.collection.find({'task_id': task_id}, filtering)

        if cursor.count() > 1:
            raise ValueError("Multiple records with same task_id found")

        if not cursor:
            raise FileNotFoundError("Record not found in database")

        record = cursor[0]

        assert task_name == record['task_name']  # nosec
        return record.get('result')

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert self.is_connected()  # nosec

        record = {
            'flow_name': flow_name,
            'node_args': node_args,
            'task_name': task_name,
            'task_id': task_id,
            'result': result

        }

        self.collection.insert(record)

        # task_id is unique here
        return task_id

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()


    def delete(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        filtering = {'_id': 0}
        cursor = self.collection.find({'task_id': task_id}, filtering)

        cursor_count = cursor.count()
        if cursor_count > 1:
            raise ValueError("Multiple records with same task_id found")

        if cursor_count == 0:
            raise SelinonMissingDataException("Record not found in database")

        self.collection.delete_one({'task_id': task_id})
