#!/usr/bin/env python

from pymongo import MongoClient
from .dataStorage import DataStorage


class MongoStorage(DataStorage):
    """
    MongoDB database Adapter
    """
    def __init__(self, host, port, db_name, collection_name):
        super(MongoStorage, self).__init__()
        self.client = None
        self.collection = None
        self.db = None
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name

    def is_connected(self):
        return self.client is not None

    def connect(self):
        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def disconnect(self):
        self.client.close()
        self.client = None
        self.db = None
        self.collection = None

    def retrieve(self, flow_name, task_name, task_id):
        filtering = {'_id': 0}
        cursor = self.collection.find({'task_id': task_id}, filtering)

        if len(cursor) > 0:
            raise ValueError("Multiple records with same task_id found")

        ret = cursor[0]

        assert(flow_name == ret['flow_name'])
        assert(flow_name == ret['task_name'])

        return ret

    def store(self, flow_name, task_name, task_id, result):
        record = {
            'flow_name': flow_name,
            'task_name': task_name,
            'task_id': task_id,
            'result': result

        }
        self.collection.insert(record)

