#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Strategy for scheduling dispatcher - system state sampling."""

import importlib

from .errors import ConfigurationError
from .helpers import check_conf_keys
from .helpers import get_function_arguments


class Strategy:
    """Sampling strategy for Dispatcher."""

    _DEFAULT_MODULE = 'selinon.strategies'
    _DEFAULT_FUNCTION = 'biexponential_increase'
    _DEFAULT_FUNC_ARGS = {'start_retry': 2, 'max_retry': 120}

    _EXPECTED_STRATEGY_FUNC_ARGS = {'status'}

    def __init__(self, module=None, function=None, func_args=None):
        """Instantiate strategy function representation.

        :param module: module from which sampling strategy should be imported
        :param function: sampling function name
        :param func_args: sampling function arguments
        """
        self.module = module or self._DEFAULT_MODULE
        self.function = function or self._DEFAULT_FUNCTION
        self.func_args = func_args or self._DEFAULT_FUNC_ARGS

    @classmethod
    def from_dict(cls, strategy_dict, flow_name):
        """Parse strategy entry.

        :param strategy_dict: strategy entry in config to be parsed
        :param flow_name: name of the flow for which the strategy function should be used.
        """
        if not strategy_dict:
            return cls()

        if not isinstance(strategy_dict, dict):
            raise ConfigurationError("Strategy not defined properly in global configuration section, expected dict,"
                                     "got %s in flow %r" % (strategy_dict, flow_name))

        if 'name' not in strategy_dict:
            raise ConfigurationError("Sampling strategy stated in global configuration but no strategy"
                                     "name defined in flow %s" % flow_name)

        if not isinstance(strategy_dict['args'], dict):
            raise ConfigurationError('Arguments to strategy function should be stated as dict,'
                                     'got %s instead in flow %r' % (strategy_dict['args'], flow_name))

        strategy_module = strategy_dict.get('import', cls._DEFAULT_MODULE)

        raw_module = importlib.import_module(strategy_module)
        raw_func = getattr(raw_module, strategy_dict['name'])

        # perform checks on args supplied
        user_args_keys = strategy_dict['args'].keys()
        func_args = set(get_function_arguments(raw_func))

        if (func_args - user_args_keys) != cls._EXPECTED_STRATEGY_FUNC_ARGS:
            raise ConfigurationError('Unknown or invalid arguments supplied to sampling strategy function,'
                                     'expected %s, got %s for strategy %r in flow %r'
                                     % ((func_args - cls._EXPECTED_STRATEGY_FUNC_ARGS), set(user_args_keys),
                                        strategy_dict['name'], flow_name))

        unknown_conf = check_conf_keys(strategy_dict, known_conf_opts=('name', 'import', 'args'))
        if unknown_conf:
            raise ConfigurationError("Unknown configuration for sampling strategy %r supplied: '%s'"
                                     % (strategy_dict['name'], unknown_conf))

        return cls(strategy_module, strategy_dict['name'], strategy_dict['args'])
