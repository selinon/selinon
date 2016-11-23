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
from selinon import Config
from selinonTestCase import SelinonTestCase


class TestFlow(SelinonTestCase):
    def test_set_config_yaml_simple(self):
        test_file = os.path.join(self.DATA_DIR, 'test_set_config.yaml')
        Config.set_config_yaml(test_file, flow_definition_files=[test_file])

        # TODO edge table is not tested
        # TODO: make sure we inspect propagated values

        tasks_available = {'task1', 'task2', 'task3'}
        flows_available = {'flow1'}
        storages_available = {'MyStorage'}

        assert tasks_available == set(Config.task_classes)
        assert tasks_available == Config.task_queues.keys()
        assert flows_available == Config.dispatcher_queues.keys()
        assert tasks_available == set(Config.task2storage_mapping.keys())
        assert storages_available == set(Config.storage_mapping.keys())
        assert storages_available == set(Config.storage2storage_cache.keys())
        assert flows_available == set(Config.node_args_from_first.keys())
        assert flows_available == set(Config.propagate_node_args.keys())
        assert flows_available == set(Config.propagate_finished.keys())
        assert flows_available == set(Config.propagate_parent.keys())
        assert flows_available == set(Config.propagate_compound_finished.keys())
        assert tasks_available == set(Config.throttle_tasks.keys())
        assert flows_available == set(Config.throttle_flows.keys())
        assert tasks_available == set(Config.max_retry.keys())
        assert tasks_available == set(Config.retry_countdown.keys())
        assert tasks_available == set(Config.storage_readonly.keys())
        assert flows_available == set(Config.nowait_nodes.keys())
        assert flows_available == set(Config.strategies.keys())

        assert 'flow1' in Config.failures
        assert {'task1'} == set(Config.nowait_nodes.get('flow1'))
        assert 'schema.json' == Config.output_schemas.get('task1')
