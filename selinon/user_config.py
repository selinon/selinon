#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""User configuration."""

import yaml

from selinon.errors import RequestError


class _ConfigSingleton(type):
    """Config singleton metaclass."""

    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_ConfigSingleton, cls).__call__(cls._config, *args, **kwargs)
        return cls._instance

    @classmethod
    def set_config(mcs, config_path):
        """Set config which should be used in within Config singleton.

        :param config_path: a path to configuration that should be used
        :type config_path: str
        """
        # set _config before the singleton is instantiated
        assert mcs._instance is None  # nosec
        mcs._config = config_path


def style_configuration(style_name):
    """Adjust style based on default style and provided style configuration (if any)."""
    def decorator(func):
        """Decorate a function call."""
        def wrapper(config_instance):
            """Wrap style configuration that makes sure style is adjusted based on user configuration."""
            property_name = '_' + func.__name__
            style_value = getattr(config_instance, property_name)

            if not style_value:
                style_value = {}
                # Apply the default style first
                default_style_value = getattr(config_instance, 'DEFAULT_' + func.__name__.upper())
                style_value.update(default_style_value)
                # Overwrite styles based on user's configuration
                # Style name could be directly determined from function name, but leave it this way for the
                # sake of readability
                config_style_value = (config_instance.raw_config.get('style') or {}).get(style_name, {})
                style_value.update(config_style_value)
                setattr(config_instance, property_name, style_value)

            return func(config_instance)

        return wrapper

    return decorator


class UserConfig(metaclass=_ConfigSingleton):
    # pylint: disable=too-many-instance-attributes
    """Configuration supplied by user."""

    DEFAULT_STYLE_TASK = {
        'style': 'filled',
        'color': 'black',
        'fillcolor': '#66cfa7',
        'shape': 'ellipse'
    }
    DEFAULT_STYLE_FLOW = {
        'style': 'filled',
        'color': 'black',
        'fillcolor': '#197a9f',
        'shape': 'box3d'
    }
    DEFAULT_STYLE_CONDITION = {
        'style': 'filled',
        'color': 'gray',
        'fillcolor': '#e8e3c8',
        'shape': 'octagon'
    }
    DEFAULT_STYLE_CONDITION_FOREACH = {
        'style': 'filled',
        'color': 'gray',
        'fillcolor': '#e8e3c8',
        'shape': 'doubleoctagon'
    }
    DEFAULT_STYLE_STORAGE = {
        'style': 'filled',
        'color': 'black',
        'fillcolor': '#894830',
        'shape': 'cylinder'
    }
    DEFAULT_STYLE_EDGE = {
        'arrowType': 'open',
        'color': 'black'
    }
    DEFAULT_STYLE_STORE_EDGE = {
        'arrowType': 'open',
        'color': '#894830',
        'style': 'dashed'
    }
    DEFAULT_STYLE_GRAPH = {
    }
    DEFAULT_STYLE_FALLBACK_EDGE = {
        'arrowType': 'open',
        'color': '#cc1010'
    }
    DEFAULT_STYLE_FALLBACK_TRUE = {
        'style': 'filled',
        'color': 'black',
        'fillcolor': '#5af47b',
        'shape': 'plain'
    }

    def __init__(self, config=None):
        """Instantiate configuration."""
        self.raw_config = {}
        self.config_path = config

        # These get assigned in style_configuration decorator.
        self._style_task = None
        self._style_flow = None
        self._style_condition = None
        self._style_condition_foreach = None
        self._style_storage = None
        self._style_edge = None
        self._style_store_edge = None
        self._style_graph = None
        self._style_fallback_edge = None
        self._style_fallback_node = None
        self._style_fallback_true = None

        if self.config_path is not None:
            try:
                with open(self.config_path) as input_file:
                    self.raw_config = yaml.load(input_file, Loader=yaml.SafeLoader)
            except Exception as exc:
                raise RequestError("Unable to load or parse style configuration file %r: %s"
                                   % (self.config_path, str(exc))) from exc

    @style_configuration('task')
    def style_task(self):
        """Return style for tasks in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_task

    @style_configuration('flow')
    def style_flow(self):
        """Return style for a flow node in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_flow

    @style_configuration('condition')
    def style_condition(self):
        """Return style for conditions in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_condition

    @style_configuration('condition_foreach')
    def style_condition_foreach(self):
        """Return style for foreach edges in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_condition_foreach

    @style_configuration('storage')
    def style_storage(self):
        """Return style for storage in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_storage

    @style_configuration('edge')
    def style_edge(self):
        """Return style for edges in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_edge

    @style_configuration('store_edge')
    def style_store_edge(self):
        """Return style for edges that lead to a storage in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_store_edge

    @style_configuration('graph')
    def style_graph(self):
        """Return style for the whole graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        return self._style_graph

    @style_configuration('fallback_edge')
    def style_fallback_edge(self):
        """Return style for fallback edges.

        :return: style definition
        :rtype: dict
        """
        return self._style_fallback_edge

    @style_configuration('fallback_true')
    def style_fallback_true(self):
        """Return style for fallback true node.

        :return: style definition
        :rtype: dict
        """
        return self._style_fallback_true
