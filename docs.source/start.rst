A Quick Start
=============

YAML configuration
******************

In order to use Selinon, you have to implement tasks and define your flow in a YAML confguration file (or split it across multiple YAML configuration files).

Naming Convention
*****************

A system consist of flows. Each flow consist of well defined dependencies on nodes that compute results. A node can be either a task or another flow, so you can make flows as nested as desired. You can make decisions on when to run which nodes based on conditions.

The key of the concept is in having a Dispatcher that schedules (dispatches work) to connected workers as defined in the flow configuration.

Imagine you defined two flows ("flow1" and "flow2") that consist of five tasks named Task1 - Task5. The flows are illustrated on images bellow.

.. image:: https://raw.github.com/fridex/selinon/master/examples/figures/flow2.png
    :align: center

In the flow "flow2" we start node Task4 on condition that is always true (we start if dispatcher was requested to start "flow2"). After Task4 finishes, we start (always) node Task5 which ends the flow "flow2". Results of tasks are stored in the database named "Storage2".

.. image:: https://raw.github.com/fridex/selinon/master/examples/figures/flow1.png
    :align: center

The second flow is slightly more complex. We (always) start with Task1. Task1 will transparently store results in Storage1. After Task1 finishes, Dispatcher checks results of Task1 in Storage1 and if condition ```result['foo'] == 'bar'``` is evaluated as True, Dispatcher starts nodes Task2 and flow2. After both Task2 and flow2 finish, Dispatcher starts Task3. If the condition ```result['foo'] == bar``` is met, Task1 is started recursively again. Results of all tasks are stored in database named "Storage1" except for results computed in subflow "flow2", where "Storage2" is used.

A YAML configuration file is available in `examples/ <https://github.com/fridex/selinon/tree/master/examples>`_. Refer to `Selinonlib <https://github.com/fridex/Selinonlib>`_ for plotting flow graphs.

Conditions
**********

Conditions are made of predicates that can be nested as desired using logical operators - `and`, `or` and `not`. See default predicates defined in `Selinonlib <https://fridex.github.io/selinonlib/api/selinonlib.predicates.html>`_ for more info.

Starting Nodes
**************

You can have a single or multiple starting nodes in your flow. If you define a single starting node, the result of starting node is propagated to other nodes as arguments. If you define more than one starting node, the result is not propagated, however you can explicitly define arguments (these arguments have higher priority in case of single starting node).

Flows
*****

Flows can be nested as desired. The only limitation is that you cannot inspect results of a flow since a flow is a black box for another flow.

Node Failures
*************

You can define fallback tasks that are run if a node fails.

Results of Tasks/Nodes
**********************

Results of tasks are stored in database as you define in your configuration file.

