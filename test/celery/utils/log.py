#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import logging


def get_task_logger(name):
    """Wrapper above Celery logging facilities"""
    return logging.getLogger(name)
