#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Configuration for caching."""

from .errors import ConfigurationError
from .helpers import check_conf_keys


class CacheConfig:
    """Configuration for Caching."""

    _DEFAULT_CACHE_NAME = 'LRU'
    _DEFAULT_CACHE_IMPORT = 'selinon.caches'
    _DEFAULT_CACHE_OPTIONS = {'max_cache_size': 0}

    def __init__(self, name, import_path, configuration, entity_name):
        """Initialize cache config as described in the YAML configuration file.

        :param name: name of the cache
        :param import_path: import from where cache should be imported
        :param configuration: cache configuration
        :param entity_name: entity for which cache should be provided
        """
        self.name = name
        self.import_path = import_path
        self.configuration = configuration
        self.entity_name = entity_name

    @property
    def var_name(self):
        """Name of variable in the generated Python code representing this cache.

        :return: name of cache variable that will be used in config.py file
        """
        return "_cache_%s_%s" % (self.entity_name, self.name)

    @classmethod
    def get_default(cls, entity_name):
        """Get default cache configuration.

        :param entity_name: entity name that will use the default cache - entity is either storage name or
                            flow name (when caching async results)
        :return: CacheConfig for the given entity
        :rtype: CacheConfig
        """
        return CacheConfig(cls._DEFAULT_CACHE_NAME, cls._DEFAULT_CACHE_IMPORT, cls._DEFAULT_CACHE_OPTIONS, entity_name)

    @classmethod
    def from_dict(cls, dict_, entity_name):
        """Parse cache configuration from a dict.

        :param dict_: dict from which cache configuration should be parsed
        :param entity_name: entity name that will use the default cache - entity is either storage name or
                            flow name (when caching async results)
        :return: cache for the given entity based on configuration
        :rtype: CacheConfig
        """
        name = dict_.get('name', cls._DEFAULT_CACHE_NAME)
        import_path = dict_.get('import', cls._DEFAULT_CACHE_IMPORT)
        configuration = dict_.get('configuration', cls._DEFAULT_CACHE_OPTIONS)

        if not isinstance(name, str):
            raise ConfigurationError("Cache configuration for '%s' expects name to be a string, got '%s' instead"
                                     % (entity_name, name))

        if not isinstance(import_path, str):
            raise ConfigurationError("Cache configuration for '%s' expects import to be a string, got '%s' instead"
                                     % (entity_name, import_path))

        if not isinstance(configuration, dict):
            raise ConfigurationError("Cache configuration for '%s' expects configuration to be a dict of cache "
                                     "configuration, got '%s' instead" % (entity_name, configuration))

        # check supplied configuration configuration
        unknown_conf = check_conf_keys(dict_, known_conf_opts=('name', 'import', 'configuration'))
        if unknown_conf:
            raise ConfigurationError("Unknown configuration configuration for cache '%s' supplied: %s"
                                     % (name, unknown_conf))

        return CacheConfig(name, import_path, configuration, entity_name)
