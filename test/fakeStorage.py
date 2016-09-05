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

from celeriac import DataStorage


class FakeStorage(DataStorage):
    """
    Fake storage helper for tests
    """
    def connect(self):
        pass

    def connected(self):
        return True

    def disconnect(self):
        pass

    def store(self, flow_name, task_name, task_id, result):
        # TODO: implement
        raise NotImplementedError()

    def retrieve(self, task_name, task_id):
        # TODO: implement
        raise NotImplementedError()

