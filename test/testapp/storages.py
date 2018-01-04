#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinon import DataStorage


class MySimpleStorage(DataStorage):
    def __init__(self, connection_string):
        super().__init__()
        self.connection_string = connection_string

    def is_connected(self):
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def retrieve(self, flow_name, task_name, task_id):
        pass

    def store(self, node_args, flow_name, task_name, task_id, result):
        pass

    def trace(self, event, msg_dict):
        pass
