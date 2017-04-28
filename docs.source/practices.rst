.. _practices:

Best Practices
==============

Here are some notes and tips on how to write YAML config files:

* Do not introduce a new flow unless it is necessary.

Each flow adds overhead for flow. If you would like to add a new flow, it should be one of the following reasons:
* I want to reuse certain implementation.
* I want to encapsulate flow so I can run fallback tasks for the whole group of tasks
* I want to throttle or optimize certain tasks (I want to let specific workers do specific flows or tasks)
* I want to recursively run the whole flow or I have dependencies on flows so they are not resolvable except copying YAML configuration logic
* I want to refer to flow as a group of tasks - a separate indivisible logic
* I need to run a flow with different arguments - each flow is defined by a group of tasks, their time and data dependencies and arguments that are propagated in the whole flow to each task (possibly except the very first one)

* Do not add flags unless you really need them.

Dispatcher implementation was desingned to be lazy. That means it was designed not to add overhead for you unleas you really need it.
This means that if you don't need flags such as `propagate_parent` or `propagate_finished` you don't need to state them in YAML file. If you state them, it will add additional computational overhead that you don't necessarily need.

* Make flow edges as minimal as possible. This will help you in the future if you would like to disable some tasks without purging queues.

Is there a way for defining flow aliases?
*****************************************

You probably saw (based on examples) that you can easily define a task alias - tasks share Python class/implementation, but have different name:

.. code-block:: yaml

  tasks:
    - name: 'Task1'
      import: 'worker.task1'

    - name: 'Task2'
      classname: 'Task1'
      import: 'worker.task1'

This is useful when you want to run same code multiple times in a flow (since nodes are referenced by names).

You can also define storage alias - useful when you want to use same database/storage adapter but with different configuration:

.. code-block:: yaml

    storages:
        - name: 'UserPostgres'
          classname: 'PostgreSQL'
          import: 'storage.storage1'
          configuration:
            host: postgres-user
            port: 5432

        - name: 'AdminPostgres'
          classname: 'PostgreSQL'
          import: 'storage.storage1'
          configuration:
            host: postgres-admin
            port: 5432

You can also create flow aliases. This is especially helpful if you want to re-use flow configuration such as edges, but you want to separate one flow to different queue (for example due to SLA, so more workers can serve serve messages). You can do so easily since YAML supports references:

.. code-block:: yaml

    flow-definitions:
        - &flow1_def
          name: 'flow1'
          queue: 'flow1_v1'
          propagate_node_args: true
          edges:
              - from:
                to:
                  - 'Task4'
              - from:
                  - 'Task4'
                to:
                  - 'Task5'

        - <<: *flow1_def
          name: 'flow1_sla'
          queue: 'flow1_sla_v1'
          # propagate_node_args and edges configuration will be shared with flow1
