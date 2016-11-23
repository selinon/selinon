Frequently Asked Questions (FAQ)
================================

Dispatcher does not work properly or hangs in an infinite loop.
***************************************************************

Check your result backend configuration in Celery. Currently, there is supported and well tested only Redis. There were found issues when rpc was used.

Can I see how does it work?
***************************

See `Selinon-demo <https://github.com/selinon/demo>`_.

I'm getting Python related errors
*********************************

Check your Python version. Selinon and Selinonlib currently support only Pyhon3+ as Celery project is about to `drop Python2 support <http://docs.celeryproject.org/en/master/whatsnew-4.0.html#last-major-version-to-support-python-2>`_.

I'm getting warning: "Multiple starting nodes found in a flow". Why?
********************************************************************

In order to propagate arguments to a flow, you should start flow with one single task (e.g. init task) which result is then propagated as an argument to each direct child tasks or transitive child tasks. This avoids various inconsistency errors and race conditions. If you define multiple starting nodes, arguments are not propagated from the first task. If you don't want to propagate arguments from an init task, you can ignore this warning for a certain flow or specify arguments explicitly in Selinon dispatcher.

How should I name tasks and flows?
**********************************

You should use names that can became part of function name (or Python3 identifier). Keep in mind that there is no strict difference between tasks and (sub)flows, so they share name space.

How can I access nested keys in a dict
**************************************

What you want is (in Python3):

.. code-block:: python

    message['foo']['bar'] == "baz"

Predicates were designed to deal with this - just provide list of keys, where position in a list describes key position:

.. code-block:: yaml

  condition:
      name: "fieldEqual"
      args:
          key:
              - "foo"
              - "bar"
          value: "baz"

I need a custom predicate
*************************

If selinonlib predicates are not suitable for you or you miss a specific predicate, you can define your own module in `global` configuration. See YAML configuration section.

What exceptions can predicates raise?
*************************************

Predicates were designed to return *always* True/False. If a condition cannot be satisfied, there is returned False. So it is safe for example to access possibly non-existing keys - predicates will return False. But there can be raised exceptions if there is problem with a database.

Do I need result backend?
*************************

Or more precisely: Do I need a result backend even when I am using my custom database for task results?

Yes, you do. Result backend is used by Celery to store information about tasks (their status, errors). Without result backend, Selinon is not capable to get information about tasks as it uses Celery. Do not use `rpc` backend as there were noted issues, but use Redis instead (or any other you prefer).

Why there is used generated code by selinonlib?
***********************************************

Since YAML config files cover some logic (such as conditions), this needs to be evaluated somehow. We could simply interpret YAML file each time, but it was easier to generate directly Python code from YAML configuration files and let Python interpret interpret it for us. Other parts from YAML file could be directly used, but mostly because of consistency and debugging the whole YAML file is used for code generation.

You can easily check how YAML file is transformed to Python code simply by running:
```
selinonlib-cli --nodes-definition NODES.yml -flow-definition FLOWS.yml -dump outputfile.py
```

How to write conditions for sub-flows?
**************************************

This is currently a limitation of Selinon. You can try to reorganize your flows so you don't need to inspect parent subflows, for most use cases it will work. Adding support for this is for future releases planned.

Is my YAML config file correct? How to improve or correct it?
*************************************************************

See Best practices section for tips.



