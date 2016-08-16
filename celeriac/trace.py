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

import logging


def _default_trace_func(msg):
    pass


def _logging_trace_func(msg):
    logging.info(msg)


class Trace(object):
    """
    Trace dispatcher work
    """
    _trace_func = _default_trace_func

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls):
        """
        Trace by using Python's logging
        """
        logging.basicConfig(level=logging.INFO)
        cls._trace_func = _logging_trace_func

    @classmethod
    def trace_by_func(cls, func):
        """
        Trace by a custom function
        :param func: function with a one single argument
        """
        cls._trace_func = func

    @classmethod
    def log(cls, msg):
        """
        Trace work
        :param msg: message to be printed
        """
        cls._trace_func(msg)

