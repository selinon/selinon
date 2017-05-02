.. _trace:

Trace Flow Actions
------------------

Selinon offers you a mechanism for tracing flow actions. By default Selinon does not output any logs, but you can configure tracing in your application. To trace by Python's logging, set ``trace`` to ``true`` in the global section of your configuration:

.. code-block:: yaml

  global:
    trace: true

Selinon will transparently print log messages of important events in your flow to stdout (informative tracepoints using ``logging.info()``) and stderr (warnings about failures using ``logging.warning()``). These logs have by default the following structure:


.. code-block:: console

  [2017-04-31 08:45:02,231: INFO/MainProcess] SELINON 8176ab3c865e - TASK_SCHEDULE : {"details": {"condition_str": "True", "countdown": null, "dispatcher_id": "f26214e6-fc2a-4e6f-97ed-6c2f6f183140", "flow_name": "myFlow", "foreach_str": null, "node_args": {"foo": "bar"}, "parent": {}, "queue": "selinon_v1", "selective": false, "selective_edge": false, "task_id": "54ec5acb-7a8f-459a-acf3-806ffe53af14", "task_name": "MyTestTask"}, "event": "TASK_SCHEDULE", "time": "2017-04-31 08:45:02.230896"}

The full list of all events with corresponding details that are captured are available in :obj:`selinon.trace` module.

.. note::

  All tasks have instantiated logging instance under ``self.log`` (see :class:`SelinonTask <selinon.selinonTask.SelinonTask>`). Feel free to use this logger to do logging.

If you would like to create your own trace logger, you can do so by registering your custom tracing function in the YAML configuration in ``global`` section:


.. code-block:: yaml

  global:
    trace:
      # from my_project.trace import my_trace_func
      function: 'my_trace_func'
      import: 'my_project.trace'

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
