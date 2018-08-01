#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Storage configuration and abstraction from YAML config file."""

from .cache_config import CacheConfig
from .errors import ConfigurationError
from .helpers import check_conf_keys


class Storage:
    """A storage representation."""

    def __init__(self, name, import_path, configuration, cache_config, class_name=None):
        # pylint: disable=too-many-arguments
        """Instantiate storage representation based on configuration supplied in YAML config files.

        :param name: storage name
        :param import_path: storage import path
        :param configuration: storage configuration that will be passed
        :param cache_config: cache configuration information
        :param class_name: storage class name
        """
        self.name = name
        self.import_path = import_path
        self.configuration = configuration
        self.class_name = class_name or name
        self.tasks = []
        self.cache_config = cache_config

    def register_task(self, task):
        """Register a new that uses this storage.

        :param task: task to be registered
        """
        self.tasks.append(task)

    @staticmethod
    def from_dict(dict_):
        """Construct storage instance from a dict.

        :param dict_: dict that should be used to instantiate Storage
        :rtype: Storage
        """
        if 'name' not in dict_ or not dict_['name']:
            raise ConfigurationError('Storage name definition is mandatory')
        if 'import' not in dict_ or not dict_['import']:
            raise ConfigurationError("Storage import definition is mandatory, storage '%s'" % dict_['name'])
        if 'configuration' not in dict_ or not dict_['configuration']:
            raise ConfigurationError("Storage configuration definition is mandatory, storage '%s'" % dict_['name'])
        if 'classname' in dict_ and not isinstance(dict_['classname'], str):
            raise ConfigurationError("Storage classname definition should be string, got '%s' instead, storage '%s'"
                                     % (dict_['classname'], dict_['name']))
        if 'cache' in dict_:
            if not isinstance(dict_['cache'], dict):
                raise ConfigurationError("Storage cache for storage '%s' should be a dict with configuration, "
                                         "got '%s' instead" % (dict_['name'], dict_['cache']))
            cache_config = CacheConfig.from_dict(dict_['cache'], dict_['name'])
        else:
            cache_config = CacheConfig.get_default(dict_['name'])

        # check supplied configuration options
        unknown_conf = check_conf_keys(dict_, known_conf_opts=('name', 'import', 'configuration', 'cache', 'classname'))
        if unknown_conf:
            raise ConfigurationError("Unknown configuration options for storage '%s' supplied: %s"
                                     % (dict_['name'], unknown_conf.keys()))

        return Storage(dict_['name'], dict_['import'], dict_['configuration'], cache_config, dict_.get('classname'))

    @property
    def var_name(self):
        """Return variable name which should be used for this storage reference in the generated Python code.

        return: name of storage variable that will be used in config.py file
        """
        return "_storage_%s" % self.name
