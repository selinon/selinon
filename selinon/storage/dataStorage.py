#!/usr/bin/env python3
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


class DataStorage(metaclass=abc.ABCMeta):
    """
    Abstract Selinon storage adapter that is implemented by a user
    """
    @abc.abstractclassmethod
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def connect(self):
        """
        Connect to a resource, if not needed, should be empty
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self):
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
    def retrieve(self, task_name, task_id):
        """
        Retrieve result stored in storage

        :param task_name: task name that result is going to be retrieved
        :param task_id: id of the task that result is going to be retrieved
        :return: task result
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store(self, node_args, flow_name, task_name, task_id, result):
        """
        Store result stored in storage

        :param node_args: arguments that were passed to node
        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be stored
        :param task_id: id of the task that result is going to be stored
        :param result: result that should be stored
        :return: unique ID of stored record
        """
        raise NotImplementedError()

    def __del__(self):
        if self.is_connected():
            self.disconnect()
