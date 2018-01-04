#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
import shlex
import pytest

from click.testing import CliRunner

from selinon_test_case import SelinonTestCase

from selinon.codename import selinon_version_codename
from selinon.version import selinon_version
from selinon.cli import execute
from selinon.cli import inspect
from selinon.cli import migrate
from selinon.cli import plot
from selinon.cli import version


class TestCli(SelinonTestCase):
    @staticmethod
    def parse_colon_separated_output(output):
        result = []

        if output[-1] == '\n':
            output = output[:-1]

        for line in output.split('\n'):
            result.append(tuple(line.split(':')))

        return result

    @staticmethod
    def check_result_files(path):
        result_files = []
        for root, _, files in os.walk(path):
            for file in files:
                result_files.append(os.path.join(root, file))

        file_sizes = map(os.path.getsize, result_files)
        assert all(size > 0 for size in file_sizes)

        return result_files

    @pytest.mark.skip
    def test_execute(self):
        # This test is skipped due to failures. To make this work,
        # https://github.com/selinon/selinon/issues/97 needs to be implemented first.
        options = shlex.split("--nodes-definition %s --flow-definitions %s --flow-name flow1 --node-args {} "
                              "--node-args-json --concurrency 1 --sleep-time 0"
                              % (os.path.join(self.DATA_DIR, 'cli', 'execute', 'nodes.yaml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'execute', 'flows')))
        # Filesystem storage expands path to result directory based on $PWD
        runner = CliRunner(env={'PWD': os.getenv('PWD', '.')})

        with runner.isolated_filesystem():
            result = runner.invoke(execute, options)
            assert result.exit_code == 0, result.output

            # Results are stored on filesystem, check that files are actually present and have non-zero size
            result_files = self.check_result_files('flow1')
            assert len(result_files) == 3

    @pytest.mark.skip
    def test_execute_selective_flow(self):
        # This test is skipped due to failures. To make this work,
        # https://github.com/selinon/selinon/issues/97 needs to be implemented first.
        options = shlex.split("--nodes-definition %s --flow-definitions %s --flow-name flow1 --node-args-file %s "
                              "--selective-task-names Task1,Task2 --sleep-time 0"
                              % (os.path.join(self.DATA_DIR, 'cli', 'execute_selective', 'nodes.yaml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'execute_selective', 'flows'),
                                 os.path.join(self.DATA_DIR, 'cli', 'execute_selective', 'node_args.json')))
        # Filesystem storage expands path to result directory based on $PWD
        runner = CliRunner(env={'PWD': os.getenv('PWD', '.')})

        with runner.isolated_filesystem():
            result = runner.invoke(execute, options)
            assert result.exit_code == 0, result.output

            # Results are stored on filesystem, check that files are actually present and have non-zero size
            result_files = self.check_result_files('flow1')
            assert len(result_files) == 2

    def test_inspect(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s"
                              % (os.path.join(self.DATA_DIR, 'cli', 'inspect', 'nodes.yml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'inspect', 'flows')))
        runner = CliRunner()
        result = runner.invoke(inspect, options)
        assert result.exit_code == 0, result.output

    def test_inspect_list_task_queues(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s --list-task-queues"
                              % (os.path.join(self.DATA_DIR, 'cli', 'inspect', 'nodes.yml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'inspect', 'flows')))
        runner = CliRunner()
        result = runner.invoke(inspect, options)

        assert result.exit_code == 0, result.output
        output = self.parse_colon_separated_output(result.output)
        assert {
            ('Task1', 'celery'),
            ('Task2', 'celery'),
            ('Task3', 'celery'),
            ('Task4', 'celery')
        } == set(output)

    def test_inspect_list_dispatcher_queues(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s --list-dispatcher-queues"
                              % (os.path.join(self.DATA_DIR, 'cli', 'inspect', 'nodes.yml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'inspect', 'flows')))
        runner = CliRunner()
        result = runner.invoke(inspect, options)

        assert result.exit_code == 0, result.output
        output = self.parse_colon_separated_output(result.output)
        assert {
            ('flow1', 'flow1_v0'),
            ('flow2', 'celery')
        } == set(output)

    def test_inspect_list_queues(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s --list-dispatcher-queues --list-task-queues"
                              % (os.path.join(self.DATA_DIR, 'cli', 'inspect', 'nodes.yml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'inspect', 'flows')))
        runner = CliRunner()
        result = runner.invoke(inspect, options)

        assert result.exit_code == 0, result.output
        output = self.parse_colon_separated_output(result.output)
        assert {
            ('flow1', 'flow1_v0'),
            ('flow2', 'celery'),
            ('Task1', 'celery'),
            ('Task2', 'celery'),
            ('Task3', 'celery'),
            ('Task4', 'celery')
        } == set(output)

    def test_migrate(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s "
                              "--old-nodes-definition %s --old-flow-definitions %s "
                              "--no-meta --migration-dir . --tainted-flows RETRY"
                              % (os.path.join(self.DATA_DIR, 'cli', 'migrate', 'new', 'nodes.yaml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'migrate', 'new', 'flows'),
                                 os.path.join(self.DATA_DIR, 'cli', 'migrate', 'old', 'nodes.yaml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'migrate', 'old', 'flows')))
        runner = CliRunner()

        with runner.isolated_filesystem():
            result = runner.invoke(migrate, options)

            assert result.exit_code == 0, result.output
            output_files = os.listdir('.')
            assert '1.json' in output_files
            assert os.path.getsize('1.json') > 0

    def test_plot(self):
        options = shlex.split("--nodes-definition %s --flow-definitions %s --output-dir . --format png"
                              % (os.path.join(self.DATA_DIR, 'cli', 'plot', 'nodes.yml'),
                                 os.path.join(self.DATA_DIR, 'cli', 'plot', 'flows')))
        runner = CliRunner()

        with runner.isolated_filesystem():
            result = runner.invoke(plot, options)

            assert result.exit_code == 0, result.output
            output_files = os.listdir('.')
            assert {'flow1.png', 'flow2.png'} == set(output_files)

    def test_version(self):
        runner = CliRunner()

        result = runner.invoke(version)
        assert result.exit_code == 0, result.output
        output = self.parse_colon_separated_output(result.output)

        for entry in output:
            assert len(entry) == 2

            if entry[0] == 'Selinon version':
                assert entry[1].strip() == selinon_version
            elif entry[0] == 'Celery version':
                # not sure if Celery is installed, simply pass this
                pass
            else:
                raise ValueError("Unknown version entry in output: %r" % entry[0])

    def test_version_codename(self):
        runner = CliRunner()
        result = runner.invoke(version, ['--codename'])
        assert result.exit_code == 0, result.output
        assert result.output.strip() == selinon_version_codename
