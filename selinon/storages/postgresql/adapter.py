#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon SQL Database adapter - PostgreSQL."""

import os
from selinon.data_storage import SelinonMissingDataException
from selinon import DataStorage

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy_utils import create_database
    from sqlalchemy_utils import database_exists
except ImportError as exc:
    raise ImportError("Please install dependencies using `pip3 install selinon[postgresql]`") from exc

from .models import Result


class PostgreSQL(DataStorage):
    """Selinon SQL Database adapter - PostgreSQL."""

    def __init__(self, connection_string, encoding='utf-8', echo=False):
        """Initialize PostgreSQL adapter from YAML configuration file.

        :param connection_string: connection string to be used to connect to PostgreSQL to
        :param encoding: encoding to be used
        :param echo: perform echo on queries
        """
        super().__init__()

        self.engine = create_engine(connection_string.format(**os.environ), encoding=encoding, echo=echo)
        self.session = None

    def is_connected(self):  # noqa
        return self.session is not None

    def connect(self):  # noqa
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        self.session = sessionmaker(bind=self.engine)()
        Result.metadata.create_all(self.engine)

    def disconnect(self):  # noqa
        if self.is_connected():
            self.session.close()
            self.session = None

    def retrieve(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        record = self.session.query(Result).filter_by(task_id=task_id).one()

        assert record.task_name == task_name  # nosec
        return record.result

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert self.is_connected()  # nosec

        record = Result(node_args, flow_name, task_name, task_id, result)
        try:
            self.session.add(record)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

        return record.id

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()

    def delete(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec

        response = self.session.query(Result).filter_by(task_id=task_id).delete()

        if response == 0:
            raise SelinonMissingDataException("Record not found")
