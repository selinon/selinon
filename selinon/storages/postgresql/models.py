#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""PostgreSQL models definition."""

try:
    from sqlalchemy import Column
    from sqlalchemy import Integer
    from sqlalchemy import Sequence
    from sqlalchemy import String
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.declarative import declarative_base
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
