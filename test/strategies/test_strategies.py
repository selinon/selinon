#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from functools import partial
from selinon.strategies import (
    linear_increase,
    linear_adapt,
    biexponential_increase,
    biexponential_decrease,
    biexponential_adapt,
    random,
    constant
)
from selinon_test_case import SelinonTestCase

# Values used in tests for strategies functions
_TEST_START_RETRY = 2
_TEST_MAX_RETRY = 20
_TEST_STEP_RETRY = 5
_TEST_PREV_RETRY = 42

# Available strategies that take 1 additional arguments
_STRATEGIES_ONE_ARG = [
    partial(constant, retry=_TEST_START_RETRY)
]

# Available strategies that take 2 additional arguments
_STRATEGIES_TWO_ARG = [
    partial(biexponential_increase, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY),
    partial(biexponential_decrease, start_retry=_TEST_START_RETRY, stop_retry=_TEST_MAX_RETRY),
    partial(biexponential_adapt, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY),
    partial(random, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY)
]


# Available strategies that take 3 additional arguments
_STRATEGIES_THREE_ARG = [
    partial(linear_increase, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY, step=_TEST_STEP_RETRY),
    partial(linear_adapt, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY, step=_TEST_STEP_RETRY)
]

_STRATEGIES_ALL_ARG = _STRATEGIES_ONE_ARG + _STRATEGIES_TWO_ARG + _STRATEGIES_THREE_ARG


@pytest.mark.parametrize("strategy", _STRATEGIES_ALL_ARG)
class TestSamplingStrategies(SelinonTestCase):
    def test_start(self, strategy):
        status = {
            'previous_retry': None,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': ['Task1', 'Task2'],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(status)
        assert retry is not None and retry > 0

    def test_only_active(self, strategy):
        status = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': [],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(status)
        assert retry is not None and retry > 0

    def test_active_and_started(self, strategy):
        status = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': ['Task3'],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(status)
        assert retry is not None and retry > 0

    def test_finish(self, strategy):
        status = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': [],
            'failed_nodes': [],
            'new_started_nodes': [],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(status)
        assert retry is None
