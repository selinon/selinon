#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import abc
from .helpers import ABC


class DataStorage(ABC):
    """
    Abstract Celeriac storage adapter that is implemented by a user
    """
    def __init__(self, configuration):
        self._configuration = configuration

    @property
    def configuration(self):
        """
        Storage configuration as defined in YAML configuration file
        """
        return self._configuration

    @abc.abstractmethod
    def connect(self):
        """
        Connect to a resource, if not needed, should be empty
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def connected(self):
        """
        :return: True if connected to a resource
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def disconnect(self):
        """
        Disconnect from a resource
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def retrieve(self, flow_name, task_name, task_id):
        """
        Retrieve result stored in storage
        :param flow_name: flow name that was used to store results
        :param task_name: task name that result is going to be retrieved
        :param task_id: id of the task that result is going to be retrieved
        :return: task result
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store(self, flow_name, task_name, task_id, result):
        """
        Store result stored in storage
        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be stored
        :param task_id: id of the task that result is going to be stored
        """
        raise NotImplementedError()
