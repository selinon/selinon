#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Indicate progress and sleep for given time."""

from math import ceil
import sys
from time import sleep


class Progress:
    """Indicate progress and sleep for given time."""

    _indicators = ('-', '\\', '|', '/')
    _current_indicator_idx = 0

    @classmethod
    def indicate(cls, iterable, show_progressbar=True, info_text=None):
        """Indicate progress on iterable.

        :param iterable: iterable that is used to iterate on progress
        :param show_progressbar: if True, there is shown a simple ASCII art spinning
        :param info_text: text that is printed on the line (progressbar follows)
        """
        for item in iterable:
            sys.stdout.write(info_text or '')
            if show_progressbar:
                sys.stdout.write(cls._indicators[cls._current_indicator_idx])
                cls._current_indicator_idx = (cls._current_indicator_idx + 1) % len(cls._indicators)

            sys.stdout.flush()
            yield item
            sys.stdout.write('\r')

        # clear whole console line at the end
        sys.stdout.write("\033[K")

    @classmethod
    def sleep(cls, wait_time, sleep_time, info_text=None, show_progressbar=True):
        """Wait and sleep for the given amount of time.

        :param wait_time: time to wait in this method in total
        :param sleep_time: time between periodic checks (parameter to sleep() function)
        :param show_progressbar: if True, there is shown a simple ASCII art spinning
        :param info_text: text that is printed on the line (progressbar follows)
        """
        if sleep_time > 0:
            total_wait_time = int(ceil(wait_time / sleep_time))
            for _ in cls.indicate(range(total_wait_time), show_progressbar, info_text=info_text,):
                sleep(sleep_time)
        elif wait_time > 0:
            sleep(wait_time)
