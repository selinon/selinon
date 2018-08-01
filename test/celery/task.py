#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################


class Task:
    def __init__(self):
        # In case of instantiating SelinonTaskEnvelope or Dispatcher, we are not passing any arguments
        self.task_name = None
        self.flow_name = None
        self.parent = None
        self.node_args = None
        self.retried_count = None
        self.queue = None
        self.countdown = None
        self.dispatcher_id = None
        self.selective = None

    @property
    def task_id(self):
        return id(self)

    def apply_async(self, kwargs, queue, countdown=None):
        from selinon.config import Config

        # Ensure that SelinonTaskEnvelope kept parameters consistent
        self.flow_name = kwargs['flow_name']
        self.node_args = kwargs.get('node_args')
        self.parent = kwargs.get('parent')
        self.dispatcher_id = kwargs.get('dispatcher_id')

        # None if we have flow
        self.task_name = kwargs.get('task_name')
        self.retried_count = kwargs.get('retried_count')
        self.countdown = countdown
        self.selective = kwargs.get('selective')

        self.queue = queue
        Config.get_task_instance.register_node(self)

        return self

    @staticmethod
    def retry(exc, max_retry):
        # ensure that we are raising with max_retry equal to 0 so we will not get into an infinite loop
        assert max_retry == 0
        raise exc

    def get_initial_system_state(self):
        """ Get initial SystemState as would Dispatcher run it inside run() method """
        from selinon.system_state import SystemState
        return SystemState(id(self), self.flow_name, node_args=self.node_args, retry=None, state=None,
                           parent=self.parent, selective=self.selective)

