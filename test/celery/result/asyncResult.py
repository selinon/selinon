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

import traceback
from .states import states


# TODO: rename finished_mapping
class AsyncResult(object):
    _finished_mapping = {}
    _result_mapping = {}

    def __init__(self, id):
        self._id = id

    def __repr__(self):
        return "<%s>" % self._id

    @classmethod
    def clear(cls):
        cls._finished_mapping = {}
        cls._result_mapping = {}

    @classmethod
    def finished_mapping(cls):
        return cls._finished_mapping

    @classmethod
    def set_finished(cls, task_id):
        cls._finished_mapping[task_id] = states.SUCCESS

    @classmethod
    def set_failed(cls, task_id):
        cls._finished_mapping[task_id] = states.FAILURE

    @classmethod
    def set_unfinished(cls, task_id):
        cls._finished_mapping[task_id] = states.STARTED

    @classmethod
    def set_result(cls, task_id, result):
        cls._result_mapping[task_id] = result

    def successful(self):
        return self._finished_mapping[self._id] == states.SUCCESS

    def failed(self):
        return self._finished_mapping[self._id] == states.FAILURE

    @property
    def result(self):
        return self._result_mapping[self._id]

    @property
    def traceback(self):
        assert self.failed()

        # there is no (documented) way on how to create Traceback object, so simulate it by rising exception
        # instance that was set
        try:
            raise self._result_mapping[self._id]
        except:
            return traceback.format_exc()

    @property
    def state(self):
        return self._finished_mapping[self._id]

    @property
    def status(self):
        return self._finished_mapping[self._id]
