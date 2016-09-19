Frequently Asked Questions (FAQ)
================================

Dispatcher does not work properly or hangs in an infinite loop.
***************************************************************

Check your result backend configuration in Celery. Currently, there is supported and well tested only Redis. There were found issues when rpc was used.

Can I see how does it work?
***************************

See `Selinon-demo <https://github.com/fridex/Selinon-demo>`_.

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

If selinonlib predicates are not suitable for you or you miss a specific predicate, you can define your own module in `global` configuration.

What exceptions can predicates raise?
*************************************

Predicates were designed to return *always* True/False. If a condition cannot be satisfied, there is returned False. So it is safe for example to access possibly non-existing keys - predicates will return False. But there can be raised exceptions if there is problem with a database.

