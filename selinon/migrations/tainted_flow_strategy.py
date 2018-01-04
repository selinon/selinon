#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Strategies used when migrations are preformed."""

from enum import Enum

from selinon.errors import UnknownError


class TaintedFlowStrategy(Enum):
    """Strategies used when migrations are preformed."""

    # Strategies need to be numbered by priority
    IGNORE = 1
    RETRY = 2
    FAIL = 3

    @classmethod
    def get_options(cls):
        """Get all options."""
        return cls.IGNORE, cls.RETRY, cls.FAIL

    @classmethod
    def get_option_names(cls):
        """Get names of all options."""
        return list(map(lambda x: x.name, cls.get_options()))

    @classmethod
    def get_default_option(cls):
        """Get default option for migration strategies."""
        return cls.IGNORE.name

    @classmethod
    def get_option_by_name(cls, name):
        """Get option by its string representation.

        :param name: option string representation
        :return: option for the given name representation
        """
        for option in (cls.IGNORE, cls.RETRY, cls.FAIL):
            if option.name.lower() == name.lower():
                return option
        raise UnknownError("Unknown tainted flow strategy %r" % name)

    @classmethod
    def get_preferred_strategy(cls, strategy1, strategy2):
        """Get preferred strategy in case of multiple migrations being done at once.

        Chose strategy with higher priority - if two migrations are done at the same time one marked as IGNORE another
        as RETRY, we have to retry flow.

        :param strategy1: the first strategy to compare with
        :type strategy1: TaintedFlowStrategy
        :param strategy2: the second strategy to compare with
        :type strategy2: TaintedFlowStrategy
        :return: strategy with higher priority
        :rtype: TaintedFlowStrategy
        """
        return max(strategy1, strategy2)
