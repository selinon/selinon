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


class StoragePool(object):
    """
    A pool that carries all database connections per worker
    """
    _database_adapters = {}

    def __init__(self, database_mapping):
        self._database_mapping = database_mapping

    @classmethod
    def set_database_adapters(cls, database_adapters):
        """
        :param database_adapters: database adapters that should be used
        """
        cls._database_adapters = database_adapters

    @property
    def database_mapping(self):
        """
        :return: current database mapping for nodes
        """
        return self._database_mapping

    def get(self, flow_name, task_name, task_id):
        # TODO: implement
        raise NotImplementedError()

    @classmethod
    def set(cls, flow_name, task_name, task_id, result):
        # TODO: implement
        raise NotImplementedError()
