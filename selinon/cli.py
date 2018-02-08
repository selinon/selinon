#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon command line interface."""
# pylint: disable=invalid-name,too-many-locals

from functools import partial
import json
import logging
import os
import sys

import click

from rainbow_logging_handler import RainbowLoggingHandler
from selinon import RequestError
from selinon import selinon_version
from selinon import selinon_version_codename
from selinon.executor import Executor
from selinon.helpers import git_previous_version
from selinon.helpers import git_previous_version_file
from selinon.migrations import Migrator
from selinon.migrations import TaintedFlowStrategy
from selinon.system import System
from selinon.user_config import UserConfig

_logger = logging.getLogger(os.path.basename(__file__))

_DEFAULT_NODE_ARGS = None
_DEFAULT_CONCURRENCY = 1
_DEFAULT_TAINTED_FLOW_STRATEGY = 'IGNORE'
_DEFAULT_PLOT_OUTPUT_DIR = '.'
_DEFAULT_PLOT_FORMAT = 'svg'
_DEFAULT_SLEEP_TIME = 0.5


def _validate_sleep_time(ctx, _, value):  # pylint: disable=unused-argument
    """Validate executor sleep time."""
    if value is not None and value < 0.0:
        raise click.BadParameter("Sleep time has to be positive number")
    return value


def _setup_logging(verbose, no_color):
    """Set up Python logging based on verbosity level.

    :param verbose: verbosity level
    :param no_color: do not use colorized output
    """
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose > 1:
        level = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(level)

    if not no_color:
        formatter = logging.Formatter("%(process)d: [%(asctime)s] %(name)s %(funcName)s:%(lineno)d: %(message)s")
        # setup RainbowLoggingHandler
        handler = RainbowLoggingHandler(sys.stderr)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def _print_version(ctx, _, value):
    """Print Selinon version and exit."""
    if not value or ctx.resilient_parsing:
        return
    click.echo(selinon_version)
    ctx.exit()


def _expand_flow_definitions(ctx, _, paths):  # pylint: disable=unused-argument
    """Expand path to configuration files if directory was supplied."""
    flow_definitions = []
    for path in paths:
        if os.path.isfile(path):
            flow_definitions.append(path)
            continue

        for root, _, files in os.walk(path, followlinks=True):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(('.yaml', '.yml')) and not file.startswith('.'):
                    _logger.debug("Found flow configuration file %r", file_path)
                    flow_definitions.append(file_path)
                else:
                    _logger.debug("File %r not considered as flow configuration file", file_path)

    return flow_definitions


@click.group()
@click.pass_context
@click.option('-v', '--verbose', count=True,
              help="Be verbose about what's going on (can be supplied multiple times).")
@click.option('--version', is_flag=True, is_eager=True, callback=_print_version, expose_value=False,
              help="Print Selinon version and exit.")
@click.option('--no-color', is_flag=True,
              help="Suppress colorized logging output.")
def cli(ctx=None, verbose=0, no_color=True):
    """Selinon command line interface."""
    if ctx:
        ctx.auto_envvar_prefix = 'SELINON'
    _setup_logging(verbose, no_color)


@cli.command()
@click.option('-n', '--nodes-definition', metavar='NODES.yml',
              type=click.Path(exists=True, dir_okay=False, readable=True), required=True,
              envvar='SELINON_NODES_DEFINITION',
              help="Path to nodes definition file.")
@click.option('-f', '--flow-definitions', metavar='FLOW.yml',
              type=click.Path(exists=True, readable=True), multiple=True, required=True,
              callback=_expand_flow_definitions, envvar='SELINON_FLOW_DEFINITIONS',
              help="Path to flow definition file (can be supplied multiple times) or path to a directory containing "
                   "YAML files.")
@click.option('--flow-name', metavar='FLOW_NAME', required=True,
              help="Specify a flow that should be run by its name.")
@click.option('-a', '--node-args', metavar='NODE_ARGS', default=_DEFAULT_NODE_ARGS,
              help="Specify arguments that should be passed to the executed flow (default: %s)." % _DEFAULT_NODE_ARGS)
@click.option('--node-args-file', metavar='FILE',
              help="Specify arguments that should be passed to the flow by a file.")
@click.option('-j', '--node-args-json', is_flag=True,
              help="Flow arguments are JSON, parse string representation into a dict.")
@click.option('-c', '--concurrency', metavar='PROCESS_COUNT', type=click.IntRange(1, None),
              default=_DEFAULT_CONCURRENCY,
              help="Worker count - number of processes that serve tasks in parallel "
                   "(default: %d)." % _DEFAULT_CONCURRENCY)
@click.option('-s', '--sleep-time', metavar='SLEEP_TIME', type=click.FLOAT, default=_DEFAULT_SLEEP_TIME,
              callback=_validate_sleep_time,
              help="Accuracy for worker sleeping when a task is scheduled to future "
                   "(default: %f)." % _DEFAULT_SLEEP_TIME)
@click.option('--config-py', type=click.Path(dir_okay=False, writable=True),
              help="Path to file where should be generated config.py placed")
@click.option('--keep-config-py', is_flag=True,
              help="Do not remove generated config.py file after run.")
@click.option('--hide-progressbar', is_flag=True,
              help="Hide progressbar during execution.")
@click.option('--selective-task-names', metavar="TASK1,TASK2,..",
              help="A comma separated list of tasks to which path should be computed on selective flow run.")
@click.option('--selective-follow-subflows', is_flag=True,
              help="Follow sub-flows in a selective flow run when computing path.")
@click.option('--selective-run-subsequent', is_flag=True,
              help="Run subsequent tasks (based on flow graph) affected by selective flow run.")
def execute(nodes_definition, flow_definitions, flow_name,
            node_args=_DEFAULT_NODE_ARGS, node_args_file=None, node_args_json=False, concurrency=_DEFAULT_CONCURRENCY,
            sleep_time=_DEFAULT_SLEEP_TIME, config_py=None, keep_config_py=False, hide_progressbar=False,
            selective_task_names=None, selective_follow_subflows=False, selective_run_subsequent=False):
    """Execute flows based on YAML configuration in a CLI."""
    if node_args and node_args_file:
        raise RequestError("Node arguments could be specified by command line argument or a file, but not from both")

    node_args = node_args
    if node_args_file:
        with open(node_args_file, 'r') as f:
            node_args = f.read()

    if node_args_json:
        try:
            node_args = json.loads(node_args)
        except Exception as e:
            raise RequestError("Unable to parse JSON arguments: %s" % str(e)) from e

    executor = Executor(nodes_definition, flow_definitions,
                        concurrency=concurrency, sleep_time=sleep_time,
                        config_py=config_py, keep_config_py=keep_config_py,
                        show_progressbar=not hide_progressbar)

    if selective_task_names:
        executor.run_flow_selective(
            flow_name,
            selective_task_names.split(','),
            node_args,
            follow_subflows=selective_follow_subflows,
            run_subsequent=selective_run_subsequent
        )
    else:
        if selective_follow_subflows:
            raise RequestError("Option --selective-follow-subflows requires --selective-task-names set")
        if selective_run_subsequent:
            raise RequestError("Option --selective-run-subsequent requires --selective-task-names set")

        executor.run(flow_name, node_args)


@cli.command()
@click.option('-n', '--nodes-definition', metavar='NODES.yml',
              type=click.Path(exists=True, dir_okay=False, readable=True), required=True,
              envvar='SELINON_NODES_DEFINITION',
              help="Path to tasks definition file.")
@click.option('-f', '--flow-definitions', metavar='FLOW.yml',
              type=click.Path(exists=True, readable=True), multiple=True, required=True,
              callback=_expand_flow_definitions, envvar='SELINON_FLOW_DEFINITIONS',
              help="Path to flow definition file (can be supplied multiple times) or path to a directory containing "
                   "YAML files.")
@click.option('-N', '--old-nodes-definition', metavar='NODES.yml',
              type=click.Path(exists=True, dir_okay=False, readable=True),
              help="Path to old nodes definition file.")
@click.option('-F', '--old-flow-definitions', metavar='FLOW.yml',
              type=click.Path(exists=True, readable=True), multiple=True,
              callback=_expand_flow_definitions,
              help="Path to old flow definition file (can be supplied multiple times) or path to a "
                   "directory containing YAML files.")
@click.option('--no-meta', is_flag=True,
              help="Do not add metadata information to generated migration files.")
@click.option('-m', '--migration-dir', type=click.Path(exists=True, writable=True, file_okay=False),
              help="Path to a directory containing generated migrations, overrides migration dir specified in "
                   "the nodes definition file.")
@click.option('-g', '--git', 'use_git', is_flag=True,
              help="Use Git VCS for obtaining old flow configuration.")
@click.option('--no-check', is_flag=True,
              help="Do not check system for errors.")
@click.option('-t', '--tainted-flows', type=click.Choice(['IGNORE', 'RETRY', 'FAIL']),
              default=_DEFAULT_TAINTED_FLOW_STRATEGY,
              help="Define strategy for tainted flows (default: %s)." % _DEFAULT_TAINTED_FLOW_STRATEGY)
def migrate(nodes_definition, flow_definitions, old_nodes_definition=None, old_flow_definitions=None, no_meta=False,
            migration_dir=None, use_git=False, no_check=False, tainted_flows=_DEFAULT_TAINTED_FLOW_STRATEGY):
    """Perform migrations on old and new YAML configuration files in flow changes."""
    # pylint: disable=too-many-branches
    if int(not old_flow_definitions) + int(not old_nodes_definition) == 1:
        raise RequestError("Please provide all flow and nodes configuration files or use --git")

    use_old_files = bool(old_flow_definitions)
    usage_clash = int(use_old_files) + int(use_git)
    if usage_clash == 2:
        raise RequestError("Option --git is disjoint with explicit configuration file specification")

    if usage_clash == 0:
        raise RequestError("Use --git or explicit old configuration file specification in order "
                           "to access old config files")

    if use_git:
        # Compute version that directly precedes the current master - there is relevant change
        # in any of config files.
        git_hash, depth = git_previous_version(nodes_definition)
        for new_flow_definition_file in flow_definitions:
            new_git_hash, new_depth = git_previous_version(new_flow_definition_file)
            if new_depth < depth:
                git_hash = new_git_hash
                depth = new_depth

        _logger.debug("Using Git hash %r for old config files", git_hash)
        old_nodes_definition = git_previous_version_file(git_hash, nodes_definition)
        old_flow_definitions = list(map(partial(git_previous_version_file, git_hash), flow_definitions))
    else:
        old_nodes_definition = old_nodes_definition
        old_flow_definitions = old_flow_definitions

    try:
        if not no_check:
            try:
                System.from_files(nodes_definition, flow_definitions)
            except Exception as e:
                raise RequestError("There is an error in your new configuration files: {}".format(str(e))) from e

            try:
                System.from_files(old_nodes_definition, old_flow_definitions)
            except Exception as e:
                raise RequestError("There is an error in your old configuration files: {}".format(str(e))) from e

        migrator = Migrator(migration_dir)
        new_migration_file = migrator.create_migration_file(
            old_nodes_definition,
            old_flow_definitions,
            nodes_definition,
            flow_definitions,
            TaintedFlowStrategy.get_option_by_name(tainted_flows),
            not no_meta
        )
    finally:
        if use_git:
            _logger.debug("Removing temporary files")
            # Clean up temporary files produced by git_previous_version()
            os.remove(old_nodes_definition)
            for old_flow_definition_file in old_flow_definitions:
                os.remove(old_flow_definition_file)

    _logger.info("New migration file placed in %r", new_migration_file)


@cli.command()
@click.option('-n', '--nodes-definition', metavar='NODES.yml',
              type=click.Path(exists=True, dir_okay=False, readable=True), required=True,
              envvar='SELINON_NODES_DEFINITION',
              help="Path to tasks definition file.")
@click.option('-f', '--flow-definitions', metavar='FLOW.yml',
              type=click.Path(exists=True, readable=True), multiple=True, required=True,
              callback=_expand_flow_definitions, envvar='SELINON_FLOW_DEFINITIONS',
              help="Path to flow definition file (can be supplied multiple times) or path to a directory containing "
                   "YAML files.")
@click.option('-c', '--config', metavar='CONFIG.yml', type=click.Path(dir_okay=False, exists=True),
              help="Path to a user configuration file containing graph styles.")
@click.option('-o', '--output-dir', default=_DEFAULT_PLOT_OUTPUT_DIR,
              help="Specify output dir where plotted graphs should be placed (default: %s)." % _DEFAULT_PLOT_OUTPUT_DIR)
@click.option('--format', 'image_format',
              help="Format of the output image (default: %s)." % _DEFAULT_PLOT_FORMAT)
def plot(nodes_definition, flow_definitions, config=None, output_dir=None, image_format=None):
    """Plot graphs of flows based on YAML configuration."""
    image_format = image_format or _DEFAULT_PLOT_FORMAT
    UserConfig.set_config(config)
    system = System.from_files(nodes_definition, flow_definitions)
    system.plot_graph(output_dir, image_format)


@cli.command()
@click.option('-n', '--nodes-definition', metavar='NODES.yml',
              type=click.Path(exists=True, dir_okay=False, readable=True), required=True,
              envvar='SELINON_NODES_DEFINITION',
              help="Path to tasks definition file.")
@click.option('-f', '--flow-definitions', metavar='FLOW.yml',
              type=click.Path(exists=True, readable=True), multiple=True, required=True,
              callback=_expand_flow_definitions, envvar='SELINON_FLOW_DEFINITIONS',
              help="Path to flow definition file (can be supplied multiple times) or path to a directory containing "
                   "YAML files.")
@click.option('-d', '--dump', metavar='DUMP.py', type=click.Path(dir_okay=False, writable=True),
              help="Generate Python code of the system.")
@click.option('--no-check', is_flag=True,
              help="Do not check system for errors.")
@click.option('--list-task-queues', is_flag=True,
              help="List all task queues based on task names.")
@click.option('--list-dispatcher-queues', is_flag=True,
              help="List dispatcher queues based on flow names.")
def inspect(nodes_definition, flow_definitions, dump=None, no_check=False,
            list_task_queues=False, list_dispatcher_queues=False):
    """Inspect Selinon configuration."""
    system = System.from_files(nodes_definition, flow_definitions, no_check)
    some_work = False

    if list_task_queues:
        for task_name, queue_name in system.task_queue_names().items():
            click.echo('%s:%s' % (task_name, queue_name))
        some_work = True

    if list_dispatcher_queues:
        for flow_name, queue_name in system.dispatcher_queue_names().items():
            click.echo('%s:%s' % (flow_name, queue_name))
        some_work = True

    if dump:
        system.dump2file(dump)
        some_work = True

    if not some_work:
        click.echo("Your configuration looks OK.")


@cli.command()
@click.option('--codename', is_flag=True,
              help="Get release codename.")
def version(codename=False):
    """Get version information."""
    try:
        from celery import __version__ as celery_version
    except ImportError:
        celery_version = 'Not Installed'

    if codename:
        click.echo(selinon_version_codename)
    else:
        click.echo('Selinon version: %s' % selinon_version)
        click.echo('Celery version: %s' % celery_version)


if __name__ == '__main__':
    cli()
