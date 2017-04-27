#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Retry exception raised on retry."""


class Retry(Exception):
    """Retry task as would Celery do except you can only specify countdown for retry."""

    def __init__(self, countdown):
        """Init retry.

        :param countdown: countdown in seconds
        """
        self.countdown = countdown
        Exception.__init__(self, countdown)
