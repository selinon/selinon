.. _optimization:

Optimization
------------

Selinon offers you a highly scalable solution. By design, you can find two optimization techniques interesting. Both are discussed in the following sections.

Flow optimization & Dispatcher scheduling
=========================================

In order to optimize your flow execution, you need to :ref:`deeply understand core concept behind Selinon <internals>`. As you already read, the key idea behind Selinon is dispatcher (see :class:`Dispacher <selinon.dispatcher.Dispatcher>`). Dispatcher is periodically scheduled and checks state of tasks in the flow and schedules new if necessary.

As there is non-zero overhead for each dispatcher run - queue message, receive message, check task status (querying result backend) and queue message for dispatcher again, it is generally a good idea to optimize dispatcher scheduling.

Selinon offers you to optimize dispatcher scheduling by configuring `sampling strategies`.

A sampling strategy basically gives information on when the dispatcher should be scheduled (or to be more precise retried) in order to check the current flow status.

By default dispatcher is rescheduled every 2 seconds. Note that it means "give dispatcher *at least* 2 seconds to retry". If you have full queues (and busy workers) your dispatcher can be rescheduled even after days.

Selinonlib offers you couple of predefined sampling strategies (refer to `Selinonlib documentation <https://selinonlib.readthedocs.io>`_ for more info):

.. code-block:: yaml

  flow-definitions:
    - name: 'flow1'
      # define sampling strategy
      sampling:
        name: 'constant'
        args:
          # check for flow state at least each 10 seconds
          retry: 10
      edges:
        # a list of edges follows

You can also provide your own sampling strategy. Basically sampling strategy is given by a number which tells Selinon when to reschedule dispatcher in the flow. This number is computed in the sampling strategy function which accepts one positional argument `status` and keywords arguments that are passed to sampling strategy function based on the YAML configuration you provided.

Now let's assume that we want to optimize scheduling dispatcher. We have the following flow definition:

.. code-block:: yaml

  flow-definitions:
    - name: 'flow1'
      edges:
        - from:
          to: 'Task1'
        - from: 'Task1'
          to: 'Task2'

Here is corresponding flow visualization:

.. image:: _static/optimization_flow1.png
  :align: center

Based on statistics we have we know that the execution time of task `Task1` is 10 seconds in average and the execution time for task `Task2` is 15 seconds in average. You can easily write your sampling strategy function:

.. code-block:: python

  import random

  def my_sampling_strategy(status, randomness):
      if not status['active_nodes']:
          return None

      if 'Task1' in status['new_started_nodes']:
          return 10 * random.uniform(-randomness, randomness)

      if 'Task2' in status['new_started_nodes']:
          return 15 * random.uniform(-randomness, randomness)

      return max(status['previous_retry'] / 2, 2)


.. note::

  This example is oversimplified. You would probably want to get more information such as what distribution task execution time has based on flow arguments and what are other parameters that affect task time execution. The argument ``randomness`` is used for demonstrating arguments propagation.


Now you can plug and use your sampling strategy function in your YAML configuration file:

.. code-block:: yaml

  flow-definitions:
    - name: 'flow1'
      sampling:
         # from myapp.sampling import my_sampling_strategy
         name: 'my_sampling_strategy'
         import: 'myapp.sampling'
         args:
           randomness: 3
      edges:
        - from:
          to: 'Task1'
        - from: 'Task1'
          to: 'Task2'

Now your sampling strategy function will be called each time dispatcher will want to reschedule. If ``None`` is returned, dispatcher should end flow immediately.  Otherwise a positive integer has to be returned that represents number of seconds for retry.

.. danger::

  As the sampling strategy function is executed by dispatcher it **can not raise any exception**! If an exception is raised, the behaviour is undefined.

Storage optimization & Distributed caches
=========================================

By using Selinon you can reach to two main issues with your cluster on heavy load:

  1. Your cluster is not powerful enough to serve requested number of tasks.
  2. Your storage/database cannot process requested numbers of requests or your network is not capable to transmit such number of queries.


In the first case the solution is simple: buy/use more hardware.

In the later one there are two main approaches how to tackle such bottleneck. You can always use more storage replicas or split data accross multiple storages and transparently configure Selinon to use different storages for different purposes (see storages aliasing in :ref:`practices`).

If the above solution is not suitable for you or you want to optimize even more, Selinon offers you an optimization that introduces distributed caches. These caches are distributed across nodes (workers) in your cluster and act like a caching mechanism to reduce number of requests to storages/databases and keep data more close to execution nodes.

Selinon by default uses cache of size 0 (no items are added to the cache). There are prepared in-memory caches like FIFO (First-In-First-Out cache), LIFO (Last-In-First-Out cache), LRU (Least-Recently-Used cache), MRU (Most-Recently-Used cache), RR (Random-Replacement cache). See `Selinonlib documentation <https://selinonlib.readthedocs.io>`_ for more info.


.. note::

  You can simply use for example Redis for caching. Just deploy Redis in the same pod as your worker and point caching mechanism to Redis adapter in your YAML configuration adapter. This way you will reduce number of requests to database as results get cached in Redis (available in the same pod) once available.

Caching task results
####################

Results of your tasks can get cached. This is especially useful when you use predicates that query storage/database often. To define a cache just provide configuration in your YAML configuration as shown bellow:

.. code-block:: yaml

  tasks:
    - name: 'Task1'
      import: 'myapp.tasks'
      cache:
        # from myapp.cache import RedisCache
        name: 'RedisCache'
        import: 'myapp.cache'
        configuration:
          host: 'redis'
          port: 6379
          db: 0
          password: 'secretpassword'
          charset: 'utf-8'
          host: 'mongo'
          port: 27017

Results are added to cache only if dispatcher requests results from cache for predicates.

.. note::

  Caching task results could be beneficial if you have a lot of conditions that depend on some task results. They could be even more beneficial if you do flow or task throttling with conditions (see :ref:`practices` for more info).

Caching task states
###################

You can also introduce caching mechanism for task states. Note that task states are handled by Celery (refer to Celery's ``AsyncResult`` for more details). Selinon offers you a way on how to place a cache as an intermediate:

.. code-block:: yaml

  flow-definitions:
    - name: 'flow1'
      cache:
        # from myapp.cache import RedisCache
        name: 'RedisCache'
        import: 'myapp.cache'
        configuration:
          host: 'redis-cache'
          port: 6379
          db: 0
          password: 'secretpassword'
          charset: 'utf-8'
          host: 'mongohost'
          port: 27017
      edges:
        - from:
          to: 'Task1'

As you can see, caches are per-flow specific and configurable. This way you can easily use caches only for flows that you consider critical for caching mechanism.

The ``RedisCache`` implementation has to derive from :class:`Cache <selinon.cache.Cache>` as well and implement required methods. Note that the configuration is passed to cache constructor similarly as in :class:`DataStorage <selinon.dataStorage.DataStorage>` case - as keyword arguments (see :ref:`storage`).

.. note::

  Caching task states is generally a good idea if you depend on many task states in your flow edges (a lot of source tasks in edges) and these tasks have various execution time (very "width" flows).

.. note::

  Due to results consistency information about task states are added to caches only if task (or flow) fails or finishes - there won't be any flow or task with the same id executed in the future.
