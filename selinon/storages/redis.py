#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon adapter for Redis database."""

import os
import json
from selinon.data_storage import SelinonMissingDataException

from selinon import DataStorage

try:
    import redis
except ImportError as exc:
    raise ImportError("Please install dependencies using `pip3 install selinon[redis]` "
                      "in order to use RedisStorage") from exc


class Redis(DataStorage):  # pylint: disable=too-many-instance-attributes
    """Selinon adapter for Redis database."""

    def __init__(self, host=None, port=6379, db=0, password=None, socket_timeout=None, connection_pool=None,
                 charset=None, errors=None, unix_socket_path=None):
        """Instantiate Redis database adapter.

        :param host: Redis host
        :param port: Redis port
        :param db: Redis database to be used
        :param password: password to be used
        :param socket_timeout: socket timeout
        :param connection_pool: redis connection pool
        :param charset: connection character set
        :param errors: error treating method
        :param unix_socket_path: path to unix socket, if any
        """
        super().__init__()
        self.conn = None
        self.host = host.format(**os.environ) if host else 'localhost'
        self.port = int(port.format(**os.environ)) if isinstance(port, str) else port
        self.db = int(db.format(**os.environ) if isinstance(db, str) else db)  # pylint: disable=invalid-name
        self.password = password.format(**os.environ)
        self.socket_timeout = socket_timeout
        self.connection_pool = connection_pool
        self.charset = charset or 'utf-8'
        self.errors = errors or 'strict'
        self.unix_socket_path = unix_socket_path

    def is_connected(self):  # noqa
        return self.conn is not None

    def connect(self):  # noqa
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db, password=self.password,
                                socket_timeout=self.socket_timeout, connection_pool=self.connection_pool,
                                charset=self.charset, errors=self.errors)

    def disconnect(self):  # noqa
        if self.is_connected():
            self.conn.connection_pool.disconnect()
            self.conn = None

    def retrieve(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        ret = self.conn.get(task_id)

        if ret is None:
            raise FileNotFoundError("Record not found in database")

        record = json.loads(ret.decode(self.charset))

        assert record.get('task_name') == task_name  # nosec
        return record.get('result')

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert self.is_connected()  # nosec

        record = {
            'node_args': node_args,
            'flow_name': flow_name,
            'task_name': task_name,
            'task_id': task_id,
            'result': result
        }

        self.conn.set(task_id, json.dumps(record))
        return task_id

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()


    def delete(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        ret = self.conn.delete(task_id)

        if ret == 0:
            raise SelinonMissingDataException("Record not found in database")
