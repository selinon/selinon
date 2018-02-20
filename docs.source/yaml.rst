.. _yaml:

.. contents:: Table of Contents
  :depth: 2

YAML configuration specification
--------------------------------

Now let's take a look at the YAML configuration file structure. At the top level, there are listed the following keys:

.. code-block:: yaml

  ---
    tasks:
      # a list of tasks available within the system
    flows:
      # a list of flows available within the system
    storages:
      # a list of storages available within the system
    global:
      # global Selinon configuration
    flow-definitions:
      # a list of flow definitions

.. note::

  If you have a lot of flows or you want to combine flows in a different way, you can place configuration of entities (`tasks`, `storages` and `flows`) into one file (called `nodes.yaml`) and flow definitions can be spread across multiple files.

Tasks
=====

Configuration of a task in the YAML configuration can look like the following example (all possible configuration keys stated):

.. code-block:: yaml

   tasks:
      - name: 'MyTask1'
        classname: 'MyTask1Class'
        import: 'myapp.tasks'
        max_retry: 5
        retry_countdown: 120
        output_schema: 'myapp/schemas/my-task1.json'
        storage: 'Storage1'
        storage_read_only: false
        storage_task_name: 'MyTaskOne'
        selective_run_function:
           function: 'my_selective_function'
           import: 'myapp.selective'
        queue: 'my_task1_queue'
        throttling:
           seconds: 10

A task definition has to be placed into `tasks` section, which consists of list of task definitions.

name
####

A name of a task. This name is used to refer to task in your flows, it is not necessarily task's class name (see ``classname`` option).

 * **Possible values:**

   * string - a task name
  
 * **Required**: true
  
import
######

Module that should be used to import task.

 * **Possible values:**

   * string - a module to be used in import statement
  
 * **Required:** true

classname
#########

Name of a class that should be imported. If omitted it defaults to ``name``.

 * **Possible values:**

   * string - task's class name

 * **Required:** false

 * **Default:** task's ``name`` configuration option

max_retry
#########

Maximum number of retries of the task (on failures - when an exception is raised) before the task is marked as *failed*.

 * **Possible values:**

   * positive integer - maximum number of retries to be performed
  
 * **Required:** false

 * **Default:** 0 - no retries on task failures are done

retry_countdown
###############

Number of seconds before a task should be retried (retry delay).

 * **Possible values:**

   * positive integer - number of seconds for retry delay
  
 * **Required:** false
  
 * **Default:** 0 - no delay is performed

output_schema
#############

JSON schema that should be used to validate results before they are stored in a storage/database. If task's result does not correspond to JSON schema, task fails and is marked as failed or retried based on the ``max_retry`` configuration option.

 * **Possible values:**

   * string - a path to JSON schema

 * **Required:** false
  
 * **Default:** None - no JSON schema validation is done on task results

storage
#######

Storage name that should be used for task results.

 * **Possible values:**

   * string - a name of storage

 * **Required:** false
  
 * **Default:** None - task results are discarded

queue
#####

Broker queue that should be used for message publishing for the given task.

Queue name can use environment variables that get expanded (e.g. ``queue: {DEPLOYMENT_PREFIX}_queue_v0`` will get expanded to ``testing_queue_v0`` if ``DEPLOYMENT_PREFIX`` environment variable is ``testing``). This allows you to parametrize resources used in deployment.

 * **Possible values:**

   * string - a name of queue

 * **Required:** false

 * **Default:** ``celery`` (celery's default queue)

storage_read_only
#################

Mark storage as read-only. Task results will not be stored to configured storage, but configured storage will be available via ``self.storage``.

 * **Possible values:**

   * boolean - true if results shouldn't be stored in the configured storage

 * **Required:** false

 * **Default:** false - results are saved to a storage if a storage was configured


storage_task_name
#################

Rename task name for :class:`Storage <selinon.storage.Storage>` operations. Selinon will perform translation of task name before storage operations get called.

 * **Possible values:**

   * string - a name/alias of task when storing task results

 * **Required:** false

 * **Default:** task's ``name`` configuration option


selective_run_function
######################

Selective run function that should be applied on :ref:`selective task runs <selective>`.

 * **Possible values:**

   * following keys for pointing to the selective run function:

     * function - a name of the function to be imported
     * import - a module name to be used for function import

 * **Required:** false

 * **Default:** :func:`selinon.routines.always_run` - a task will be always forced to run on selective task runs

throttling
##########

Task execution throttling configuration. See :ref:`Optimization section <optimization>` for more detailed explanation.

  * **Possible values:**

    * following keys for time delay configuration, each configurable using a positive integer, if omitted defaults to 0:

      * days
      * seconds
      * microseconds
      * milliseconds
      * minutes
      * hours
      * weeks

  * **Required:** false

  * **Default:** all time delay configuration keys set to zero - no throttling is performed

Storages
========

Here is an example of a storage configuration with all the configuration options:

.. code-block:: yaml

    storages:
      - name: 'Storage1'
        import: 'myapp.storages'
        classname: 'SqlStorage'
        cache:
          name: 'Cache1'
          import: 'myapp.caches'
          configuration:
            size: 10
        configuration:
          connection_string: 'postgresql://postgres:postgres@localhost:5432/mydatabase'
          echo: false

A storage definition has to be placed into `storages` section, which is a list of storage definitions.

name
####

A name of a storage. This name is used to refer storage in tasks.

 * **Possible values:**

   * string - a name of the storage
  
 * **Required:** true

import
######

Module that holds storage class definition.

 * **Possible values:**

   * string - a module to be used to import storage

 * **Required:** true

classname
#########

A name of the database storage adapter class to be imported.

 * **Possible values:**

   * string - a name of the class to import

 * **Required:** false

 * **Default:** storage ``name`` configuration option

configuration
#############

Configuration that will be passed to storage adapter instance. This option depends on database adapter implementation, see :ref:`storage adapter implementation <storage>` section.

cache
#####

Cache to be used for result caching, see :ref:`cache <yaml-cache>` section and the :ref:`optimization objective <optimization>`.

Flow definition
===============

A flow definition is placed into a list of flow definitions in the YAML configuration file.

.. code-block:: yaml

  flow-definitions:
    - name: 'flow1'
      propagate_parent:
        - 'flow2'
      node_args_from_first: true
      #propagate_compound_finished:
      max_retry: 2
      retry_countdown: 10
      propagate_finished:
        - 'flow2'
      propagate_node_args:
        - 'flow2'
      nowait:
       - 'Task1'
      eager_failures:
       - 'Task2'
      cache:
        name: 'Cache1'
        import: 'myapp.caches'
        configuration:
          size: 10
      sampling:
        name: 'constant'
        args:
          # check for flow state each 10 seconds
          retry: 10
      throttling:
         seconds: 10
      edges:
        - from:
            - 'Task1'
          to:
            - 'Task2'
            - 'flow2'
          condition:
            name: 'fieldEqual'
            args:
              key:
                - 'foo'
                - 'bar'
              value: 'baz'
        - from: 'flow2'
          to: 'Task3'
        - from: 'Task3'
          to: 'flow3'
          condition:
            name: 'fieldEqual'
            args:
              key:
                - 'foo'
              value: 'bar'
          foreach:
            import: 'myapp.foreach'
            function: 'iter_foreach'
            # result of the function would be used as sub-flow arguments to flow3
            propagate_result: false
      failures:
        nodes:
          - 'Task1'
        fallback:
          - 'Fallback1'

name
####

A name of the flow. This name is used to reference the flow.

 * **Possible values:**

   * string - a name of the flow
  
 * **Required:** true

propagate_parent
################

Propagate parent nodes to sub-flow or sub-flows. Parent nodes will be available in the ``self.parent`` property of the :class:`SelinonTask <selinon.selinon_task.SelinonTask>` class and it will be possible to transparently transparently query results using :class:`SelinonTask <selinon.selinon_task.SelinonTask>` methods.

 * **Possible values:**

   * string - a name of the flow to which parent nodes should be propagated
   * list of strings - a list of flow names to which parent nodes should be propagated
   * boolean - enable or disable parent nodes propagation to all sub-flows
  
 * **Required:** false
  
 * **Default:** false - do not propagate parent to any sub-flow

propagate_finished
##################

Propagate finished nodes from sub-flows. Finished nodes from sub-flows will be available in the ``self.parent`` of the :class:`SelinonTask <selinon.selinon_task.SelinonTask>` class property as a dictionary and it will be possible to transparently query results using :class:`SelinonTask <selinon.selinon_task.SelinonTask>` methods. All tasks will be recursively received from all sub-flows of the inspected flow.

 * **Possible values:**

   * string - a name of the flow from which finished nodes should be propagated
   * list of strings - a list of flow names from which finished nodes should be propagated
   * boolean - enable or disable finished nodes propagation from all sub-flows
  
 * **Required:** false
  
 * **Default:** false - do not propagate finished nodes at all from any sub-flow

propagate_compound_finished
###########################

Propagate finished nodes from sub-flows in a compound (flattened) form - see :ref:`patterns` for more info. Finished nodes from sub-flows will be available in the ``self.parent`` of the :class:`SelinonTask <selinon.selinon_task.SelinonTask>` class property as a dictionary and it will be possible to transparently query results using :class:`SelinonTask <selinon.selinon_task.SelinonTask>` methods. All tasks will be recursively received from all sub-flows of the inspected flow.

 * **Possible values:**

   * string - a name of the flow from which finished nodes should be propagated
   * list of strings - a list of flow names from which finished nodes should be propagated
   * boolean - enable or disable finished nodes propagation from all sub-flows

 * **Required:** false

 * **Default:** false - do not propagate finished nodes at all from any sub-flow

propagate_node_args
###################

Propagate node arguments to sub-flows.

 * **Possible values:**

   * string - a name of flow to which node arguments should be propagated
   * list of strings - a list of flow names to which node arguments should be propagated
   * boolean - enable or disable node arguments propagation to all sub-flows
  
 * **Required:** false
  
 * **Default:** false - do not propagate flow arguments to any sub-flow

node_args_from_first
####################

Use result of the very first task as flow arguments. There *has to be* only one starting task if this configuration option is set.

 * **Possible values:**

   * boolean - enable or disable result propagation as a flow arguments

 * **Required:** false

 * **Default:** false - do not propagate result of the first task as flow arguments

nowait
######

Do not wait for a node (a task or a sub-flow) to finish. This node cannot be stated as a dependency in the YAML configuration file. Note that node failure will not be tracked if marked as ``nowait``.

This option is an optimization - if all tasks that are not stated in `nowait` finish, dispatcher will schedule nowait nodes and marks the flow finished/failed (based on task/fallback success) and will *not* retry.

 * **Possible values:**

   * string - a node that should be started with nowait flag
   * list of strings - a list of nodes that should be started with nowait flag
  
 * **Required:** false
  
 * **Default:** an empty list - wait for all nodes to complete in order to end flow

eager_failures
##############

If a node stated in `eager_failures` fails, dispatcher will immediately stop scheduling new nodes and marks flow as failed without checking results of other nodes inside flow.

In case there is configure `max_retry` configuration option, flow will be restarted respecting `max_retry` configuration option.

  * **Possible values:**

   * string - a node that failure can cause the whole flow eager failure
   * list of strings - a list of nodes that can cause eager flow failure (if any node from the list fails)
   * bool - if set to true any node failure inside a flow will cause eager flow failure

  * **Required:** false

 * **Default:** an empty list (or false) - do not stop scheduling eagerly on any failure

max_retry
#########

Maximum number of retries of the flow in case of flow failures. A flow can fail when one of nodes is marked as failed (task or any sub-flow). In case of retries, all tasks are scheduled from the beginning as in the first run.

 * **Possible values:**

   * positive integer - maximum number of retries to be performed

 * **Required:** false

 * **Default:** 0 - no retries on flow retries are done

retry_countdown
###############

Number of seconds before a flow should be retried (retry delay).

 * **Possible values:**

   * positive integer - number of seconds for retry delay

 * **Required:** false

 * **Default:** 0 - no delay is performed

sampling
########

Define a custom module where dispatcher sampling strategy function (see :ref:`optimization` for more info).

  * **Possible values:**

    * ``name`` - a name of sampling strategy function to be used

        **Default:** ``biexponential_increase``

    * ``import`` - a module name from which the sampling strategy function should be imported

        **Default:** ``selinon.strategies``

    * ``args`` - additional sampling strategy configuration options passed as keyword arguments to the sampling strategy

        **Default:**

          * ``start_retry: 2``

          * ``max_retry: 120``

  * **Required:** false

  * **Defaults:** as listed in each configuration key

Refer to :mod:`selinon.strategies` for additional info.

throttling
##########

Flow execution throttling configuration. See :ref:`Optimization section <optimization>` for more detailed explanation.

  * **Possible values:**

    * following keys for time delay configuration, each configurable using a positive integer, if omitted defaults to 0:

      * days
      * seconds
      * microseconds
      * milliseconds
      * minutes
      * hours
      * weeks

  * **Required:** false

  * **Default:** all time delay configuration keys set to zero - no throttling is performed

cache
#####

Cache to be used for node state caching, see :ref:`cache <yaml-cache>` section and the :ref:`optimization objective <optimization>`.

edges
#####

A list of edges describing dependency on nodes. See `Flow edge definition`_.

Flow edge definition
====================

A flow consist of time or data dependencies between nodes that are used in the flow. These dependencies are modeled using edges which are conditional and can have multiple source and multiple destination nodes (tasks or flows).

from
####

A source node or nodes of the edge. If no source edge is provided, the edge is considered to be a starting edge (the ``from`` keyword however needs to be explicitly stated). There can be multiple starting edges in a flow.

 * **Possible values:**

   * string - name of the source node - either a task name or a flow name
   * list of strings - a list of names of source nodes
   * None - no source nodes, edge is a starting edge
  
 * **Required:** true
  
to
##

 * **Possible values:**

   * string - name of the destination node - either a task name or a flow name
   * list of strings - a list of names of destination nodes

 * **Required:** true

condition
#########

A condition made of predicates that determines whether the edge should be fired (destination nodes should be scheduled). Boolean operators `and`, `or` and not can be used as desired to create more sophisticated conditions.

 * **Possible values:**

   * ``and`` - N-ary predicate that is true if all predicates listed in the list are true
   * ``or`` - N-ary predicate that is true if any predicate listed in the list is true
   * ``not`` - unary predicate that is true if listed predicate is false
   * ``name`` - a reference to a leaf predicate to be used, this predicate is imported from predicates module defined in the ``global`` section
  
 * **Required:** false
 * **Default:** ``alwaysTrue()`` predicate defined in :mod:`selinon.predicates.alwaysTrue` which always evaluates to true

If ``name`` is used, there are possible following configuration options:

  * node - name of the node to which the given condition applies, can be omitted if there is only one source node
  * args - arguments that should be passed to predicate implementation as keyword arguments

An example of a condition definition:

.. code-block:: yaml

  condition:
    #or:
    and:
      - name: 'fieldEqual'
        node: 'Task1'
        args:
          key: 'foo'
          value: 'bar'
      - not:
          name: 'fieldExist'
          node: 'Task2'
          args:
            key: 'baz'
            value: 42

Please refer to the ``predicates`` module available in :mod:`selinon.predicates`. This module states default predicates that could be immediately used. You can also provide your own predicates by configuring used module in the global_ configuration section.

foreach
#######

Spawn multiple (let's say N, where N is a variable determinated on run time) nodes. The foreach function will be called iff ``condition`` is evaluated as true. See :ref:`patterns` for more info.

  * **Possible values:**

    * foreach function definition:
       * ``function`` - a name of the function that should be used
       * ``import`` - a module from which the foreach function should be imported
    * ``propagate_result`` - if true (defaults to false), result of the foreach function will be propagated to sub-flows (cannot be propagated to tasks), this option is disjoint with ``propagate_node_args``

  * **Required:** false

  * **Default:** None
  
Flow failures
=============

A list of failures that can occur in the system and their fallback nodes.

 * **Possible values:**

   * a list of failures each item defining:

     * ``nodes`` - a node name or a list of node names that can trigger fallback scheduling in case of failure
     * ``fallback`` - a node name or a list of node names (a task name or flow names) that should be scheduled in case of failure
     * ``condition`` - condition that would be evaluated, if true the fallback is triggered; see condition definition on task flow edges for more info and examples
  
 * **Required:** false
  
 * **Default:** an empty list of failures - all failures will be propagated to parent flows


An example of a failure definition:

.. code-block:: yaml

  failures:
     - nodes:
         - 'Task1'
         - 'Task2'
       fallback: 'Fallback1'

     - nodes: 'Task1'
       fallback:
         - 'Fallback1'
         - 'Fallback2'

Failures are greedy, if multiple fallbacks can be run, there is used failure that covers as mush as possible of the failed nodes.


.. note::

  * fallbacks are run once there are no active nodes in the flow - dispatcher is trying to recover from failures in this place
  * there is scheduled one fallback at the time - this prevents from time dependency in failures
  * there is always chosen failure based how many nodes you expect to fail - dispatcher is greedy with fallback - that means it always choose failure that is dependent on highest number of nodes; if multiple failures can be chosen, lexical order of node names comes in place
  * a flow fails if there is still a node that failed and there is no failure specified to recover from failure
  * fallback on fallback is fully supported (and nested as desired)

global
======

Global configuration section for Selinon.

predicates_module
#################

Define a custom predicate module. There will be imported predicates from this module (using predicate ``name``).

 * **Possible values:**

   * string - a predicate module from which predicates module should be imported

 * **Required:** false

 * **Default:** :mod:`selinon.predicates`

default_task_queue
##################

Default queue for tasks. This queue will be used for all tasks (overrides default Celery queue), unless you specify ``queue`` in the task definition, which has the highest priority.

The queue name can be parametrized using environment variables - see `queue`_ configuration for more info.

  * **Possible values:**

    * string - a queue name for tasks

  * **Required:** false

  * **Default:** ``celery`` - Celery's default queue


default_dispatcher_queue
########################

Queue for dispatcher task. This queue will be used for all dispatcher tasks (overrides default Celery queue), unless you specify ``queue`` in the flow definition, which has the highest priority.

The queue name can be parametrized using environment variables - see `queue`_ configuration for more info.

  * **Possible values:**

    * string - a queue for dispatcher to schedule flows

  * **Required:** false

  * **Default:** ``celery`` - Celery's default queue

trace
#####

Keep track of actions that are done in flow. See :ref:`trace` for more info with configuration examples.

  * **Possible values:**

    * an array where each entry configures tracing mechanism used

      * ``function`` - register a callback function that is called on each event, configuration options:

        * ``import`` - import to be used to import tracing function
        * ``name`` - name of function to be imported

      * ``logging`` - use Python's logging facilities, configuration options:

        * ``true`` (boolean) - turn on Python's logging

      * ``sentry``  - use Sentry for monitoring task failures (only events of type ``TASK_FAILURE``), configuration options:

        * ``dsn`` - Sentry's DSN to describe target service and Sentry's project to log to, can be parametrized based on environment variables similarly as queues - see `queue`_ configuration for more info.

      * ``storage`` - use storage adapter to store traced events, configuration options:

        * ``name`` - name of storage to be used
        * ``method`` - name of method to call on storage adapter instance

      * ``json`` - trace directly to a JSON

        * not parameterizable - accepts only a boolean - e.g. ``json: true`` to turn JSON tracing on, all tracepoints are one-liners so they are consumable to ELK (Elastic Seach+Logstash+Kibana) of (Elastic Search+Fluentd+Kibana) stack for later log inspection

  * **Required:** false

  * **Default:** do not trace flow actions

migration_dir
#############

A path to directory containing generated migrations. See :ref:`migrations` for more info.

A name of migration directory can be parametrized using environment variables - see `queue`_ configuration for more info on how to reference environment variables.

  * **Possible values:**

    * string - a path to migration directory

  * **Required:** false

  * **Default:** no migration directory - no migrations will be performed


cache
=====

Define cache for result caching or for task state caching - see distributed caches in :ref:`optimization` section. Each cache has to be of type :class:`Cache <selinon.cache.Cache>`.

name
####

Name of the cache class to be imported.

  * **Possible values:**

    * string - a name of the cache class

  * **Required:** false

  * **Default:** None - no cache is used

import
######

Name of the module from which the cache should be imported.

  * **Possible values:**

    * string - a name of the cache class

  * **Required:** false

  * **Default:** None - no cache is used


configuration
#############

Additional configuration options that are passed to the cache constructor as keyword arguments. These configuration options depend on particular cache implementation.
