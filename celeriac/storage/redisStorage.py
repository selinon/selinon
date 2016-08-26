#!/usr/bin/env python

from .dataStorage import DataStorage


class RedisStorage(DataStorage):
    def __init__(self):
        super(RedisStorage, self).__init__()
        raise NotImplementedError()

    def is_connected(self):
        raise NotImplementedError()

    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    def retrieve(self, flow_name, task_name, task_id):
        raise NotImplementedError()

    def store(self, flow_name, task_name, task_id, result):
        raise NotImplementedError()

