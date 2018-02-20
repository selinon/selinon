#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
from selinon import Config
from selinon_test_case import SelinonTestCase
from selinon.global_config import GlobalConfig


class TestConfig(SelinonTestCase):
    def test_parse_trace_sentry_without_environ(self):
        expected_dsn = "https://without_environ:secret@sentry.io/project"
        dns = {'dsn': expected_dsn}
        GlobalConfig._parse_trace_sentry(dns)
        assert expected_dsn == GlobalConfig._trace_sentry[0]
        GlobalConfig._trace_sentry.pop()
        
    def test_parse_trace_sentry_with_environ(self):
        expected_dsn = "https://from_environ:secret@sentry.io/project"
        os.environ["DSN"] = expected_dsn
        dns = {'dsn': '{DSN}'}
        GlobalConfig._parse_trace_sentry(dns)
        assert expected_dsn == GlobalConfig._trace_sentry[0]
        GlobalConfig._trace_sentry.pop()


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
        assert tasks_available | flows_available == set(Config.max_retry.keys())
        assert tasks_available | flows_available == set(Config.retry_countdown.keys())
        assert tasks_available == set(Config.storage_readonly.keys())
        assert flows_available == set(Config.nowait_nodes.keys())
        assert flows_available == set(Config.strategies.keys())

        assert 'flow1' in Config.failures
        assert {'task1'} == set(Config.nowait_nodes.get('flow1'))
        assert 'schema.json' == Config.output_schemas.get('task1')

    def test_set_config_dict_simple(self):
        nodes = {
            'tasks': [
                {
                    'name': 'Task1',
                    'import': 'testapp.tasks'
                },
                {
                    'name': 'task2',
                    'import': 'testapp.tasks'
                }
            ],
            'flows': [
                'flow1'
            ]
        }

        flows = [
            {
                'flow-definitions': [
                    {
                        'name': 'flow1',
                        'edges': [
                            {
                                'from': None,
                                'to': 'Task1'
                            },
                            {
                                'from': 'Task1',
                                'to': 'task2'
                            }
                        ]
                    }
                ]
            }
        ]

        Config.set_config_dict(nodes, [flows])
