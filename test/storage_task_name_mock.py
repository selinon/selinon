#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


class StorageTaskNameMock:
    """Mock storage_task_name dict from configuration"""
    def __init__(self):
        pass

    def __getitem__(self, item):
        # Return what we have asked - try to pretend {'TaskX': 'TaskX'} for each task
        return item
