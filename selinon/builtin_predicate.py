#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Built-in predicates used as core building blocks to build predicates."""

import abc
import ast
from functools import reduce

from .errors import ConfigurationError
from .predicate import Predicate


class BuiltinPredicate(Predicate, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Build in predicate abstract class."""

    pass


class NaryPredicate(BuiltinPredicate, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """N-ary predicate abstract class."""

    def __init__(self, children):
        """Instantiate N-ary predicate.

        :param children: children (predicates) of this predicate
        """
        super().__init__()
        self._children = children

    def _str(self, operator):
        """Create a string representation of this predicate.

        :param operator: boolean operator that should be used for concatenating child predicates
        :return: string representation (Python code)
        :rtype: str
        """
        ret = ""
        for child in self._children:
            if ret:
                ret += " %s " % operator
            ret += str(child)

        if len(self._children) > 1:
            ret = "(" + ret + ")"

        return ret

    @staticmethod
    def _create(tree, cls, nodes_from, flow, can_inspect_results):
        """Instantiate N-ary predicate class.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param cls: class of type NaryPredicate
        :param nodes_from: nodes that are used in described edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: instance of cls
        """
        if not isinstance(tree, list):
            raise ConfigurationError("Nary logical operators expect list of children")
        children = []
        for child in tree:
            children.append(Predicate.construct(child, nodes_from, flow, can_inspect_results))
        return cls(children)

    def predicates_used(self):
        """Compute predicates that are used (transitively).

        :return: used predicates by children
        """
        return reduce(lambda x, y: x + y.predicates_used(), self._children, [])

    def nodes_used(self):
        """Compute nodes that are used (transitively).

        :return: list of nodes that are used
        :rtype: List[Node]
        """
        return reduce(lambda x, y: x + y.nodes_used(), self._children, [])

    def check(self):
        """Check predicate for consistency."""
        for child in self._children:
            child.check()

    def requires_message(self):
        """Check whether any of child predicates requires results of nodes.

        :return: True if any of the children require results of parent task
        """
        return any(child.requires_message() for child in self._children)


class UnaryPredicate(BuiltinPredicate, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Unary predicate abstract class."""

    def __init__(self, child):
        """Instantiate unary predicate.

        :param child: child (predicate) of this predicate
        """
        super().__init__()
        self._child = child

    @staticmethod
    def _create(tree, cls, nodes_from, flow, can_inspect_results):
        """Instantiate N-ary predicate class.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param cls: class of type NaryPredicate
        :param nodes_from: nodes that are used in described edge
        :param flow: flow to which predicate belongs to
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: instance of cls
        """
        if isinstance(tree, list):
            raise ConfigurationError("Unary logical operators expect one child")
        return cls(Predicate.construct(tree, nodes_from, flow, can_inspect_results))

    def predicates_used(self):
        """Compute all predicates that are used (transitively) by child/children.

        :return: used predicates by children
        """
        return self._child.predicates_used()

    def nodes_used(self):
        """Compute all nodes that are used (transitively) by child/children.

        :return: list of nodes that are used
        :rtype: List[Node]
        """
        return self._child.nodes_used()

    def check(self):
        """Check predicate for consistency."""
        self._child.check()

    def requires_message(self):
        """Check whether any of child predicate(s) requires results of nodes (transitively).

        :return: True if the child requires results of parent task
        """
        return self._child.requires_message()


class AndPredicate(NaryPredicate):
    """And predicate representation."""

    def __str__(self):
        """Create a string representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """
        return "(" + reduce(lambda x, y: str(x) + ' and ' + str(y), self._children) + ")"

    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """
        return ast.BoolOp(ast.And(), [ast.Expr(value=x.ast()) for x in self._children])

    @staticmethod
    def create(tree, nodes_from, flow, can_inspect_results):
        """Create And predicate.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: instance of cls
        """
        return NaryPredicate._create(tree, AndPredicate, nodes_from, flow, can_inspect_results)


class OrPredicate(NaryPredicate):
    """And predicate representation."""

    def __str__(self):
        """Create a string representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """
        return "(" + reduce(lambda x, y: str(x) + ' or ' + str(y), self._children) + ")"

    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """
        return ast.BoolOp(ast.Or(), [ast.Expr(value=x.ast()) for x in self._children])

    @staticmethod
    def create(tree, nodes_from, flow, can_inspect_results):
        """Create Or predicate.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: instance of cls
        """
        return NaryPredicate._create(tree, OrPredicate, nodes_from, flow, can_inspect_results)


class NotPredicate(UnaryPredicate):
    """Unary or predicate representation."""

    def __str__(self):
        """Create a string representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """
        return "(not %s)" % str(self._child)

    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """
        return ast.UnaryOp(ast.Not(), ast.Expr(value=self._child.ast()))

    @staticmethod
    def create(tree, nodes_from, flow, can_inspect_results):
        """Create Or predicate.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :param can_inspect_results: True if predicates in the condition can query task result
        :type can_inspect_results: bool
        :return: instance of cls
        """
        return UnaryPredicate._create(tree, NotPredicate, nodes_from, flow, can_inspect_results)


class AlwaysTruePredicate(BuiltinPredicate):
    """Predicate used if condition in config file is omitted."""

    def __init__(self, flow):
        """Instantiate predicate that holds always True.

        :param flow: flow for which this predicate is used
        """
        super().__init__()
        self.flow = flow

    def __str__(self):
        """Create string representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """
        return "True"

    def predicates_used(self):  # noqa
        return []

    def nodes_used(self):  # noqa
        return []

    def check(self):  # noqa
        """Check predicate for consistency."""
        pass

    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """
        # We should return:
        #   return ast.NameConstant(value=True)
        # but it does not work with codegen
        return ast.Name(id='True', ctx=ast.Load())

    @staticmethod
    def create(tree, nodes_from, flow, can_inspect_results):  # noqa
        return AlwaysTruePredicate(flow)

    def requires_message(self):  # noqa
        return False
