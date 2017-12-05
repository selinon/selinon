.. _tasks:

Task implementation
-------------------

Each task you want to run in Selinon has to be of a type :obj:`SelinonTask <selinon.selinon_task>`.

The only thing you need to define is the ``run()`` method which accepts ``node_args`` parameter based on which this task computes its results. The return value of your Task is after that checked against JSON schema (if configured so) and stored in a database or a storage if a storage was assigned to the task in the YAML configuration.

.. code-block:: python

  from selinon import SelinonTask

  class MyTask(SelinonTask):
      def run(self, node_args):
          # compute a + b
          return {'c': node_args['a'] + node_args['b']}


Now you need to point to the task implementation from YAML configuration files (``nodes.yaml``):

.. code-block:: yaml

  tasks:
    # Transcripts to:
    #   from myapp.tasks import MyTask
    - name: 'MyTask'
      import: 'myapp.tasks'

See :ref:`YAML configuration <yaml>` section for all possible configuration options.

In order to retrieve data from parent tasks or flows you can use prepared :class:`SelinonTask <selinon.selinon_task.SelinonTask>` methods. You can also access configured storage and so.

Task failures
#############

First, make sure you are familiar with retry options that can be passed in the :ref:`YAML configuration <yaml>`.

If your task should not be rescheduled due to a fatal error, raise :exc:`FatalTaskError <selinon.errors.FatalTaskError>`. This will cause fatal task error and task will not be rescheduled. Keep in mind that `max_retry` from YAML configuration file **will be ignored**! If you want to retry, just raise any appropriate exception that you want to track in trace logs.

In case you want to reschedule your task without affecting ``max_retry``, just call ``self.retry()``. Optional argument ``countdown`` specifies countdown in seconds for rescheduling. Note that this method is not fully compatible with Celery's `retry mechanism <http://docs.celeryproject.org/en/latest/reference/celery.app.task.html#celery.app.task.Task.retry>`_.

Check :class:`SelinonTask <selinon.selinon_task.SelinonTask>` code documentation.

Some implementation details
###########################

Here are some implementation details that are not necessary helpful for you:

* :obj:`SelinonTask <selinon.selinon_task>` is not Celery task
* the constructor of the task is transparently called by :obj:`SelinonTaskEnvelope <selinon.selinon_taskEnvelope>`, which handles flow details propagation and also :ref:`Selinon tracepoints <trace>`
* :obj:`SelinonTaskEnvelope <selinon.selinon_taskEnvelope>` is of type `Celery task <http://docs.celeryproject.org/en/latest/userguide/tasks.html#custom-task-classes>`_
