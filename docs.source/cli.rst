.. _cli:

Command line interface
----------------------

Selinon is shipped with a CLI (Command Line Interface) that offers you interaction with Selinon code and perform various actions like plotting flow graphs, executing a flow via a command line executor or inspect provided configuration via sub-commands.

Most of the sub-commands accept configuration files you use with Selinon so to use Selinon CLI you need to pass ``nodes.yaml`` file as well as all your flow configuration files (if you split configuration files into multiple YAML files).

.. code-block:: bash

  usage: selinon-cli [-h] [--verbose] [--no-color]
                        {execute,migrate,plot,inspect,version} ...

  developer interaction tool for Selinon config files

  optional arguments:
    -h, --help            show this help message and exit
    --verbose, -v         be verbose about what's going on (can be supplied
                          multiple times)
    --no-color, -n        suppress colorized logging output

  sub-commands:
    {execute,migrate,plot,inspect,version}
      execute             execute flow locally without Celery
      migrate             generate config file migration
      plot                plot graphs from configuration files
      inspect             collect useful information from configuration files
      version             get version info and exit


.. note::

  These the config files you pass to :class:`Config <selinon.config.Config>` class on Selinon initialization that holds Selinon configuration.


.. note::

  If you do queue name expansion based on environment variables, you need to explicitly make sure the environment variable is present. Otherwise parsing configuration files will fail.

Inspecting configuration
========================

Selinon CLI offers you the ``inspect`` sub-command. This sub-command parses all the config files and outputs requested results.

You can use the ``inspect`` sub-command for example to list all queues stated in your YAML files or do other stuff for querying parsed and checked configuration files.

One of the interesting options worth to state is the ``--dump`` command. As Selinon uses YAML config files that carry some logic, there is needed interpretation of some parts. Thus there is generated Python code from YAML configuration files that is afterwards supplied to Selinon itself to orchestrate flows. You can request the generated Python code with the ``--dump`` command if you are interesting in it or you just want to explore Selinon internals.


Plotting flow graphs
====================

As flows and dependencies between tasks might get pretty complex, Selinon offers you a way to visualize flows. For this purpose the ``plot`` sub-command is available in Selinon CLI. It can plot flow graphs for you in any format that is supported by `graphviz <https://pypi.python.org/pypi/graphviz>`_.

All the flow graphs available in this documentation were plotted using the ``plot`` sub-command.

You can also adjust style of the resulting image by supplying a YAML-based configuration that states style information. As Selinon uses `graphviz <https://pypi.python.org/pypi/graphviz>`_ under the hood to plot flow graphs, all options that are supported by graphviz are applicable to nodes, edges, storages and arrows. In the example bellow, you can see how to adjust style configuration of different components that occur in the resulting flow plot (this is default configuration).

.. code-block:: yaml

    # A task node.
    task:
        style: filled
        color: black
        fillcolor: '#66cfa7'
        shape: ellipse
    # A flow node (a sub-flow in a flow).
    flow:
        style: filled
        color: black
        fillcolor: '#197a9f'
        shape: box3d
    # A condition on a simple edge.
    condition:
        style: filled
        color: gray
        fillcolor: '#e8e3c8'
        shape: octagon
    # A condition on foreach edge.
    condition_foreach:
        style: filled
        color: gray
        fillcolor: '#e8e3c8'
        shape: doubleoctagon
    # A storage node.
    storage:
        style: filled
        color: black
        fillcolor: '#894830'
        shape: cylinder
    # A simple edge.
    edge:
        arrowtype: open
        color: black
    # An edge from a task to storage that was assigned to the task.
    store_edge:
        arrowtype: open
        color: '#894830'
        style: dashed
    # An edge that leads to a fallback node.
    fallback_edge:
        arrowtype: open
        color': '#cc1010'
    # A special mark signalizing to always recover from a failure (fallback set to true).
    fallback_true:
        style: filled
        color: black
        fillcolor: '#5af47b'
        shape: plain

You can find more configuration options in the `graphviz library documentation <https://pypi.python.org/pypi/graphviz>`_.

Simulating flow execution
=========================

To debug, explore, play or interact with task flow execution anyhow, Selinon CLI offers you a built in executor. This executor tries to simulate message queueing and message consuming so no broker (and Celery's result backend) is involved.

.. note::

  Note that the execution can vary from real broker interaction as there are involved other parameters as well (e.g. prefetch multiplier configuration, concurrent broker message publishing, etc.).

Executor currently supports only single-process, single threaded executor - one worker serving tasks. Worker accepts messages in a round-robin fashion based on message availability in queues.

In order to see what is happening during executor run, you can run executor in a verbose mode. Executor in that case prints all the execution actions. It can help you when you want to experiment with your flow configuration or you would like to debug strange flow behaviour.

Generating migrations of configuration files
============================================

As Selinon offers you a mechanism to do changes in your configuration files and do re-deployment of workers, there needs to be a mechanism that ensures changes done in your configuration files are reflected to already present messages on queue. This lead to migrations design.

You can generate migration files using the ``migrate`` sub-command. Please take a look to the :ref:`section that explains migrations in more detail <migrations>`.
