.. _trace:

Trace flow actions
------------------

Selinon offers you a mechanism for tracing flow actions. By default Selinon does not output any logs, but you can configure tracing in your application. To trace by Python's logging, set ``trace`` to ``true`` in the global section of your configuration:

.. code-block:: yaml

  global:
    trace:
      - logging: true

Selinon will transparently print log messages of important events in your flow to stdout (informative tracepoints using ``logging.info()``) and stderr (warnings about failures using ``logging.warning()``). These logs have by default the following structure:


.. code-block:: console

  [2017-04-31 08:45:02,231: INFO/MainProcess] SELINON 8176ab3c865e - TASK_SCHEDULE : {"details": {"condition_str": "True", "countdown": null, "dispatcher_id": "f26214e6-fc2a-4e6f-97ed-6c2f6f183140", "flow_name": "myFlow", "foreach_str": null, "node_args": {"foo": "bar"}, "parent": {}, "queue": "selinon_v1", "selective": false, "selective_edge": false, "task_id": "54ec5acb-7a8f-459a-acf3-806ffe53af14", "task_name": "MyTestTask"}, "event": "TASK_SCHEDULE", "time": "2017-04-31 08:45:02.230896"}

The full list of all events with corresponding details that are captured are available in :obj:`selinon.trace` module.

.. note::

  All tasks have instantiated logger instance under ``self.log`` (see :class:`SelinonTask <selinon.selinonTask.SelinonTask>`). Feel free to use this logger to do logging.

If you would like to create your own trace logger, you can do so by registering your custom tracing function in the YAML configuration in the ``global`` section:


.. code-block:: yaml

  global:
    trace:
      # from myapp.trace import my_trace_func
      - function:
          name: 'my_trace_func'
          import: 'myapp.trace'

The tracing function has two arguments - ``event`` and ``msg_dict``. Argument ``event`` semantically corresponds to the tracepoint that is being captured and ``msg_dict`` captures all the details related to the tracepoint (see :obj:`selinon.trace`):

.. code-block:: python

  from selinon import Trace

  def my_trace_func(event, msg_dict):
      if event == Trace.FLOW_FAILURE:
          print("My flow %s failed" % msg_dict['flow_name'])

.. danger::

  Note that **raising exceptions in the tracing function leads to undefined behaviour**.

.. note::

  If you are using ELK (Elastic Search, Logstash, Kibana) stack for aggregating logs, check `python-logstash <https://pypi.python.org/pypi/python-logstash>`_.

Selinon also offers you to put trace events to a storage. For this purpose you can define the following configuration entry:

.. code-block:: yaml

  global:
    trace:
      - storage:
          name: 'MyStorage'
          method: 'trace'

By providing the configuration option stated above, Selinon will call ``MyStorage.trace()`` method on each event. Note that the storage needs to be defined in the ``storage`` section in ``nodes.yaml``, Selinon will automatically instantiate storage adapter and connect to the storage/database once needed.

As you can see, the ``trace`` section consists of list of tracing mechanisms being used. You can define as many tracing entries as you want.

Sentry integration
==================

If you would like to use `Sentry <https://sentry.io>`_ for monitoring, you can use already existing support. Selinon reports all ``TASK_FAILURE`` events to the Sentry instance if you provide the following configuration:


.. code-block:: yaml

  global:
    trace:
      - sentry:
          dsn: 'http://5305e373726b40ca894d8cfd121dea34:78c848fac46040d1a3218cc0bf8ef6a7@sentry:9000/2'

You need to adjust the `Sentry DSN <https://docs.sentry.io/quickstart/#configure-the-dsn>`_ configuration so it points to correctly set up Sentry instance. You can browse `Selinon demo <https://github.com/selinon/demo>`_ to see Sentry integration in action.

Also don't forget to install raven dependency explicitly so Sentry integration works:

.. code-block:: console

  $ pip3 install raven
