#!/usr/bin/env python3

from sqlalchemy import create_engine, Column, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.orm import sessionmaker
from .dataStorage import DataStorage

Base = declarative_base()


class Result(Base):
    __tablename__ = 'result'

    id = Column(Integer, Sequence('result_id'), primary_key=True)
    flow_name = Column(String(128))
    task_name = Column(String(128))
    task_id = Column(String(255), unique=True)
    # We are using JSONB for postgres, if you want to use other database, change column type
    result = Column(JSONB)
    node_args = Column(JSONB)

    def __init__(self, node_args, flow_name, task_name, task_id, result):
        self.flow_name = flow_name
        self.task_name = task_name
        self.task_id = task_id
        self.result = result
        self.node_args = node_args


class SqlStorage(DataStorage):
    def __init__(self, connection_string, encoding='utf-8', echo=False):
        super(SqlStorage, self).__init__()

        self.engine = create_engine(connection_string, encoding=encoding, echo=echo)
        self.session = None

    def is_connected(self):
        return self.session is not None

    def connect(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        self.session = sessionmaker(bind=self.engine)()
        Base.metadata.create_all(self.engine)

    def disconnect(self):
        if self.is_connected():
            self.session.close()
            self.session = None

    def retrieve(self, task_name, task_id):
        assert(self.is_connected())

        record = self.session.query(Result).filter_by(task_id=task_id).one()

        assert(record.task_name == task_name)
        return record.result

    def store(self, node_args, flow_name, task_name, task_id, result):
        assert(self.is_connected())

        record = Result(node_args, flow_name, task_name, task_id, result)
        try:
            self.session.add(record)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

        return record.id

