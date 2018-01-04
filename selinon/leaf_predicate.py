#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Leaf predicate in condition - should always return True/False for the given input."""

import ast
import importlib
import logging

from .errors import ConfigurationError
from .global_config import GlobalConfig
from .helpers import dict2strkwargs
from .helpers import get_function_arguments
from .predicate import Predicate


class LeafPredicate(Predicate):
    """Leaf predicate representation."""

    _logger = logging.getLogger(__name__)

    def __init__(self, predicate_func, node, flow, args=None):
        """Instantiate leaf predicate.

        :param predicate_func: predicate function
        :param node: node that predicate conforms to
        :param flow: flow to which predicate belongs to
        :param args: predicate arguments that should be used
        """
        super().__init__()
        self.node = node
        self.flow = flow

        self._func = predicate_func
        self._args = args or {}
        self._func_args = get_function_arguments(self._func)

        if self.requires_message() and not self.node:
            raise ConfigurationError("Expected task name for predicate '%s'" % self._func.__name__)

    def requires_message(self):
        """Check whether this predicate requires results from parent node.

        :return: True if predicate requires a message from a parent node
        """
        return 'message' in self._func_args

    def requires_node_args(self):
        """Check whether this predicate inspects arguments passed to flow.

        :return: True if predicate requires a node arguments
        """
        return 'node_args' in self._func_args

    def _check_parameters(self):
        """Check user defined predicate parameters against predicate parameters.

        :raises: ConfigurationError
        """
        func_args = get_function_arguments(self._func)
        user_args = self._args.keys()

        if 'message' in func_args:
            # message argument is implicit and does not need to be specified by user
            func_args.remove('message')

        if 'node_args' in func_args:
            # node_args are implicit as well
            func_args.remove('node_args')

        func_args = set(func_args)
        user_args = set(user_args)

        error = False

        for arg in func_args - user_args:
            self._logger.error("Argument '%s' of predicate '%s' not specified in flow '%s'",
                               arg, self._func.__name__, self.flow.name)
            error = True

        for arg in user_args - func_args:
            self._logger.error("Invalid argument '%s' for predicate '%s' in flow '%s'",
                               arg, self._func.__name__, self.flow.name)
            error = True

        if error:
            raise ConfigurationError("Bad predicate arguments specified in flow '%s'" % self.flow.name)

    def _check_usage(self):
        """Check correct predicate usage.

        :raises: ValueError
        """
        if self.requires_message() and self.node and self.node.is_flow():
            raise ConfigurationError("Results of sub-flows cannot be used in predicates")
        if self.requires_message() and not self.node:
            raise ConfigurationError("Cannot inspect results in starting edge in predicate '%s'" % self._func.__name__)
        if self.requires_message() and not self.node.storage:
            raise ConfigurationError("Cannot use predicate '%s' that requires a results "
                                     "of node '%s' (import: %s) since this node has no storage assigned"
                                     % (str(self), self.node.name, self.node.import_path))

    def check(self):
        """Check whether predicate is correctly used.

        :raises: ValueError
        """
        self._check_usage()
        self._check_parameters()

    def __str__(self):
        """Create a string representation of this predicate (Python function call).

        :return: a string representation of this predicate
        """
        if self.requires_message():
            if self._args:
                return "%s(db.get('%s'), %s)"\
                       % (self._func.__name__, self._task_str_name(), dict2strkwargs(self._args))

            return "%s(db.get('%s'))" % (self._func.__name__, self._task_str_name())

        # we hide node_args parameter
        return "%s(%s)" % (self._func.__name__, dict2strkwargs(self._args))

    def _task_str_name(self):  # noqa
        # task_name can be None if we have starting edge
        if self.node is None:
            return 'None'

        return "%s" % self.node.name

    def ast(self):
        """Create Python AST of this predicate.

        :return: AST representation of predicate
        """
        # we could directly use db[task] in predicates, but predicates should not handle database errors,
        # so leave them on higher level (selinon) and index database before predicate is being called

        kwargs = []
        # we want to avoid querying to database if possible, if a predicate does not require message, do not ask for it
        if self.requires_message():
            # this can raise an exception if check was not run, since we are accessing storage that can be None
            kwargs.append(ast.keyword(arg='message',
                                      value=ast.Call(func=ast.Attribute(value=ast.Name(id='db', ctx=ast.Load()),
                                                                        attr='get', ctx=ast.Load()),
                                                     args=[ast.Str(s=self._task_str_name())],
                                                     keywords=[], starargs=None, kwargs=None)))
        if self.requires_node_args():
            kwargs.append(ast.keyword(arg='node_args', value=ast.Name(id='node_args', ctx=ast.Load())))

        kwargs.extend([ast.keyword(arg=k, value=ast.Str(s=v)) for k, v in self._args.items()])

        return ast.Call(func=ast.Name(id=self._func.__name__, ctx=ast.Load()),
                        args=[], starargs=None, kwargs=None, keywords=kwargs)

    def predicates_used(self):
        """Return a list of predicates that are used.

        :return: list of predicates that are used
        :rtype: List[Predicate]
        """
        return [self._func] if self._func else []

    def nodes_used(self):
        """Return a list of nodes that are used by this predicate.

        :return: list of nodes that are used
        :rtype: List[Node]
        """
        return [self.node] if self.node else []

    @classmethod
    def create(cls, name, node, flow, args=None):  # pylint: disable=arguments-differ
        """Create predicate.

        :param name: predicate name
        :type name: str
        :param node: node to which predicate belongs
        :type node: Node
        :param flow: flow to which predicate belongs
        :type flow: Flow
        :param args: predicate arguments
        :return: an instantiated predicate
        :raises: ImportError
        """
        try:
            module = importlib.import_module(GlobalConfig.predicates_module)
            predicate = getattr(module, name)
        except ImportError:
            cls._logger.error("Cannot import predicate '%s'", name)
            raise
        return LeafPredicate(predicate, node, flow, args)
