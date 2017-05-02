.. _patterns:

Useful Flow Patterns
--------------------

TODO: this section needs to be revisited

This section gives you an overview if flow patterns that can be suitable for your flow.

Permutations of all Parent Tasks - Cyclic Diamond with Permutations
###################################################################

Consider following flow definition (tasks definition omitted, but all Task2 and Task3 use storage, Task4 is reading parent results):

A visualization of such flow using Selinonlib tool would be:

.. code-block:: yaml

    Task1
    
  Task2 Task3

    Task4


As one would expect Task4 is called after Task2 and Task3 are finished. Consider task1_1 is task id of Task1 after it's first run, task2_1 is tasks id of Task2 after it's first, task2_2 is task id after it's second run and so forth. After the second run of Task1, Task2 and Task3 (before Task4 execution) we would have following task ids:

`task1_1`
`task1_2`
`task2_1`
`task2_2`
`task3_1`
`task3_2`
`task4_1`

Now we Task4 is going to be run. In the first run, it was run with parent:

.. code-block:: json

  {'Task2': 'task2_1', 'Task3': 'task3_1'}

Now Task4 will be run three times with parent

.. code-block:: json

  {'Task2': 'task2_1', 'Task3': 'task3_2'}

.. code-block:: json

  {'Task2': 'task2_2', 'Task3': 'task3_1'}

.. code-block:: json

  {'Task2': 'task2_2', 'Task3': 'task3_2'}

In other words, task Task4 will be run with all permutations of Task2 and Task3. If this is not what you want, take a look at diamond without permutations pattern listed bellow.

Cyclic Diamond without Permutations
###################################

If you would like to run Task4 with parent:

.. code-block:: json

  {'Task2': 'task2_1', 'Task3': 'task3_1'}

.. code-block:: json

  {'Task2': 'task2_2', 'Task3': 'task3_2'}

you have to run Task2 and Task3 in a separate flow that is marked with `propagate_finished` (and `propagate_node_args` if needed).

Task Aggregator
###############

A task's parent is a dictionary a task id under task name key of parent and a dictionary of finished nodes if parent is a flow. An example of parent dictionary is:

.. code-block:: json

  {
    'Task1': 'task1'
    ...
    'flow1': {
      'Task2': ['task2_1', 'task2_2']
      ...
    }
  }

If you would like to aggregate results of all tasks of a type in another task (e.g. if they are computed recursively), just run task in a separate flow. After a flow finishes, you will get list of task ids under `parent['flow']['Task1']`.

Flow Failures versus Task Failures
##################################

You can define failures based on nodes. This means that you can define failure on a task-level or on a flow-level. If you would like to recover from flow failure, encapsulate your flow into a another flow and run fallback from it. The fallback's parent are all parents that succeeded in parent flow, so you have available results of tasks that succeeded and wrote results to database (if configured so).

If you would like to recover on task level inside flow, it would be easier for you to handle failures on task level.

