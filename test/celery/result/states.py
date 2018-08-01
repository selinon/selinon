#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


class states:
    """
    Task states as defined in Celery (only name conforms)
    """
    PENDING = 1
    RECEIVED = 2
    STARTED = 3
    SUCCESS = 4
    FAILURE = 5
    REVOKED = 6
    REJECTED = 7
    RETRY = 8
    IGNORED = 9
