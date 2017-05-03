.. _storage:

Storage adapter implementation
------------------------------

Currently, there are available prepared database adapters, see `Selinonlib <https://github.com/selinon/selinonlib>`_ module. In order to use these storages, you have to manually install database adapters as they are not explicitly included by requirements.

  * `SqlStorage` - SQLAlchemy adapter for SQL databases

    .. code-block:: yaml

      storages:
        - name: 'MySqlStorage'
          classname: 'SqlStorage'
          import: 'selinon.storage'
          configuration:
            connection_string: 'postgres://postgres:postgres@postgres:5432/postgres'
            encoding: 'utf-8'
            echo: false

  * `RedisStorage` - Redis database adapter

    .. code-block:: yaml

      storages:
        - name: 'MyRedisStorage'
          classname: 'RedisStorage'
          import: 'selinon.storage'
          configuration:
            host: 'redishost'
            port: 6379
            db: 0
            password: 'secretpassword'
            charset: 'utf-8'
            host: 'mongohost'
            port: 27017

  * `MongoStorage` - MongoDB database adapter

    .. code-block:: yaml

      storages:
        - name: 'MyMongoStorage'
          classname: 'MongoStorage'
          import: 'selinon.storage'
          configuration:
            db_name: 'database_name'
            collection_name: 'collection_name'
            host: 'mongohost'
            port: 27017

  * `S3` - AWS S3 database adapter

    .. code-block:: yaml

      storages:
        - name: 'MyMongoStorage'
          classname: 'S3Storage'
          import: 'selinon.storage'
          configuration:
            db_name: 'database_name'
            collection_name: 'collection_name'
            host: 'mongohost'
            port: 27017


Using a custom storage adapter
##############################

You can define your own storage by inheriting from :class:`DataStorage <selinon.dataStorage.DataStorage>` abstract class:

::

  from selinon import DataStorage

  class MyStorage(DataStorage):
      def __init__(self, host, port):
          # arguments from YAML file are pasased to constructor as key-value arguments
          pass

      def is_connected():
          # predicate used to check connection
          return False

      def connect():
          # define how to connect based on your configuration
          pass

      def disconnect():
          # define how to disconnect from storage
          pass

      def retrieve(self, flow_name, task_name, task_id):
          # define how to retrieve results
          pass

      def store(self, flow_name, task_name, task_id, result):
          # define how to store results
          pass

      def store_error(self, node_args, flow_name, task_name, task_id, exc_info):
          # optionally define how to track errors/task failures if you need to
          pass

And pass this storage to Selinon in your YAML configuration:

.. code-block:: yaml

  storages:
    # from myapp.storages import MyStorage
    - name: 'MyStorage'
      import: 'myapp.storages'
      configuration:
        host: 'localhost'
        port: '5432'

You can also reuse `Selinonlib <https://github.com/selinon/selinonlib>`_ implementation of storages in order to define your custom ``retrieve()`` and ``store()`` methods based on your requirements.

Database connection pool
########################

Each worker is trying to be efficient when it comes to number of connections to a database. There is held only one instance of :class:`DataStorage <selinon.dataStorage.DataStorage>` class per whole worker. Selinon transparently takes care of concurrent-safety when calling methods of :class:`DataStorage <selinon.dataStorage.DataStorage>` if you plan to run your worker with concurrency level higher than one.


.. note::

  You can also simply share connection across multiple :class:`DataStorage <selinon.dataStorage.DataStorage>` classes in inheritance hierarchy and reuse already defined connections. You can also do storage aliasing as described in :ref:`practices`.
