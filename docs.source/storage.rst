.. _storage:

Storage adapter implementation
------------------------------

Currently, there are available prepared database adapters, see `Selinonlib <https://github.com/selinon/selinon>`_ module. In order to use these storages, you have to manually install database adapters using extras as they are not explicitly included by requirements.

`SqlStorage` - SQLAlchemy adapter for SQL databases
===================================================

.. code-block:: console

  pip3 install selinon[postgresql]

A configuration example:

.. code-block:: yaml

  storages:
    - name: 'MySqlStorage'
      classname: 'PostgreSQL'
      import: 'selinon.storages.postgresql'
      configuration:
        connection_string: 'postgres://postgres:postgres@postgres:5432/postgres'
        encoding: 'utf-8'
      echo: false

The connection string can be parametrized using environment variables. The implementation is available in :mod:`selinon.storages.postgresql`.

`Redis` - Redis database adapter
=======================================

.. code-block:: console

  pip3 install selinon[redis]

A configuration example:

.. code-block:: yaml

  storages:
    - name: 'MyRedisStorage'
      classname: 'Redis'
      import: 'selinon.storages.redis'
      configuration:
        host: 'redishost'
        port: 6379
        db: 0
        charset: 'utf-8'
      port: 27017

Configuration entries `host`, `port`, `password` and `db` can be parametrized using environment variables. The implementation is available in :mod:`selinon.storages.redis`.

`MongoDB` - MongoDB database adapter
=========================================

.. code-block:: console

  pip3 install selinon[mongodb]

A configuration example:

.. code-block:: yaml

  storages:
    - name: 'MyMongoStorage'
      classname: 'MongoDB'
      import: 'selinon.storages.mongodb'
      configuration:
        db_name: 'database_name'
        collection_name: 'collection_name'
        host: 'mongohost'
      port: 27017

Configuration entries `db_name`, `collection_name`, `host` and `port` can be parametrized using environment variables. The implementation is available in :mod:`selinon.storages.mongodb`.


`S3` - AWS S3 database adapter
==============================

.. code-block:: console

      `pip3 install selinon[s3]`

A configuration example:

.. code-block:: yaml

  storages:
    - name: 'MyS3Storage'
      classname: 'S3Storage'
      import: 'selinon.storages.s3'
      configuration:
        bucket: 'my-bucket-name'
        aws_access_key_id: 'AAAAAAAAAAAAAAAAAAAA'
        aws_secret_access_key: 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'
        region_name: 'us-east-1'

Configuration entries `bucket`, `aws_access_key_id`, `aws_secret_access_key`, `region_name`, `location`, `use_ssl` and `endpoint_url` can be parametrized using environment variables. The implementation is available in :mod:`selinon.storages.s3`.

.. note::

  You can use awesome projects such as `Ceph Nano <https://github.com/ceph/cn>`_, `Ceph <https://ceph.com/>`_ or `Minio <https://min.io/>`_ to run your application without AWS. You need to adjust `endpoint_url` configuration entry of this adapter to point to your alternative. You can check `Selinon's demo deployment <https://github.com/selinon/demo-deployment>`_ for more info.

In memory storage
=================

A configuration example:

.. code-block:: yaml

  storages:
    - name: 'Memory'
      classname: 'InMemoryStorage'
      import: 'selinon.storages.memory'
      configuration:
        echo: false

No additional requirements are necessary to be installed. This storage adapter stores results in memory. It is suitable for use with Selinon CLI and executor where you just want to run a flow and check results. As results are stored in memory, it is not possible to scale number of workers in many cases as results are stored in memory of a node.

The implementation is available in :mod:`selinon.storages.memory`.

Few notes on using adapters
===========================

If you want to you multiple adapters, you can specify multiple adapters in extras when installing:

.. code-block:: console

  pip3 install selinon[mongodb,postgresql,s3]

Note that spaces are not allowed in extras (also escape brackets when using zsh).

Using a custom storage adapter
##############################

You can define your own storage by inheriting from :class:`DataStorage <selinon.data_storage.DataStorage>` abstract class:

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

      def delete(self, flow_name, task_name, task_id):
          # define how to delete results
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

If you create an adapter for some well known storage and you feel that your adapter is generic enough, feel free to share it with community by opening a pull request!

Database connection pool
########################

Each worker is trying to be efficient when it comes to number of connections to a database. There is held only one instance of :class:`DataStorage <selinon.data_storage.DataStorage>` class per whole worker. Selinon transparently takes care of concurrent-safety when calling methods of :class:`DataStorage <selinon.data_storage.DataStorage>` if you plan to run your worker with concurrency level higher than one.


.. note::

  You can also simply share connection across multiple :class:`DataStorage <selinon.data_storage.DataStorage>` classes in inheritance hierarchy and reuse already defined connections. You can also do storage aliasing as described in :ref:`practices`.

If you would like to request some storage from your configuration, you can request storage adapter from Selinon :class:`StoragePool <selinon.storage_pool>`:

.. code-block:: python

   from selinon import StoragePool

   # Name of storage was set to MyMongoStorage in nodes.yaml configuration file (section storages).
   mongo = StoragePool.get_connected_storage('MyMongoStorage')

Selinon will transparently take care of instantiation, connection and sharing connection pool across the whole process. Check out other useful methods of :class:`StoragePool <selinon.storage_pool>`.


.. note::

  If there is anything wrong with storage or storage adapters causing dispatcher failing to determine the next steps in the flow, dispatcher is retried respecting the flow's ``retry_countdown`` configuration option. This way you will not lose messages that cannot be consumed due to storage errors. However if a task cannot write or read from a storage, it is marked as failed.
