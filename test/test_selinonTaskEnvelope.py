#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import os
import pytest
from jsonschema import ValidationError
from selinonTestCase import SelinonTestCase
from selinon.selinonTaskEnvelope import SelinonTaskEnvelope


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
