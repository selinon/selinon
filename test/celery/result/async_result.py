#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import traceback
from .states import states


# TODO: rename finished_mapping
class AsyncResult:
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
