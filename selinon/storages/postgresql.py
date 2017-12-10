#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon SQL Database adapter - PostgreSQL."""

from selinon import DataStorage

try:
    from sqlalchemy import (create_engine, Column, Integer, Sequence, String)
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy_utils import create_database, database_exists
except ImportError as exc:
    raise ImportError("Please install dependencies using `pip3 install selinon[postgresql]`") from exc

_Base = declarative_base()  # pylint: disable=invalid-name


class Result(_Base):
    """Record for a task result."""

    __tablename__ = 'result'

    id = Column(Integer, Sequence('result_id'), primary_key=True)  # pylint: disable=invalid-name
    flow_name = Column(String(128))
    task_name = Column(String(128))
    task_id = Column(String(255), unique=True)
    # We are using JSONB for postgres, if you want to use other database, change column type
    result = Column(JSONB)
    node_args = Column(JSONB)

    def __init__(self, node_args, flow_name, task_name, task_id, result):  # noqa
        self.flow_name = flow_name
        self.task_name = task_name
        self.task_id = task_id
        self.result = result
        self.node_args = node_args


class PostgreSQL(DataStorage):
    """Selinon SQL Database adapter - PostgreSQL."""

    def __init__(self, connection_string, encoding='utf-8', echo=False):
        """Initialize PostgreSQL adapter from YAML configuration file.

        :param connection_string:
        :param encoding:
        :param echo:
        """
        super().__init__()

        self.engine = create_engine(connection_string, encoding=encoding, echo=echo)
        self.session = None

    def is_connected(self):  # noqa
        return self.session is not None

    def connect(self):  # noqa
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        self.session = sessionmaker(bind=self.engine)()
        _Base.metadata.create_all(self.engine)

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
