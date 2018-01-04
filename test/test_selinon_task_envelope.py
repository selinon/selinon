#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
import pytest
from flexmock import flexmock
from jsonschema import ValidationError
from selinon_test_case import SelinonTestCase
from selinon.task_envelope import SelinonTaskEnvelope
from request_mock import RequestMock


class TestSelinonTaskEnvelope(SelinonTestCase):
    def test_validate_schema(self):
        edge_table = {}
        task_name = 'Task1'
        result = {'projectName': 'Selinon'}
        schema_path = os.path.join('test', 'data', 'validate_schema.json')

        self.init(edge_table, output_schemas={'Task1': schema_path})

        # we shouldn't raise anything
        SelinonTaskEnvelope.validate_result(task_name, result)

    def test_validate_schema_error(self):
        edge_table = {}
        task_name = 'Task1'
        result = {'foo': 'bar'}
        schema_path = os.path.join('test', 'data', 'validate_schema.json')

        self.init(edge_table, output_schemas={'Task1': schema_path})

        with pytest.raises(ValidationError):
            SelinonTaskEnvelope.validate_result(task_name, result)

    def test_selinon_retry(self):
        task = SelinonTaskEnvelope()
        task.request = RequestMock()
        flexmock(task).should_receive('retry').and_return(ValueError)

        params = {
            'task_name': 'Task1',
            'flow_name': 'flow1',
            'parent': {},
            'node_args': None,
            'retry_countdown': 0,
            'retried_count': 0,
            'dispatcher_id': '<dispatcher-id>',
            'user_retry': False
        }

        try:
            # we need to make context for traceback, otherwise this test will fail on Python3.4
            raise KeyError()
        except KeyError:
            # we should check params here
            with pytest.raises(ValueError):
                task.selinon_retry(**params)
