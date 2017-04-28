.. _faq:

Frequently Asked Questions (FAQ)
--------------------------------

I see only one contributor, should I trust you?
***********************************************

A lot of people on conferences asked me this question. The answer is: No, never trust anyone!

There is currently one contributor, but every project started somehow. Selinon was designed for `fabric8-analytics project at Red Hat <https://github.com/fabric8-analytics>`_, where it is still used and it already served (I believe) hundreds of thousands tasks or flows (if not more). If you find Selinon interesting for your use-case, feel free to use it (and buy me some beer or at least let me know `that you like it or any experiences you have <https://saythanks.io/to/fridex>`_).

If you find a bug, place for enhancement or anything where I can be helpful, feel free to let me know. Even you can be Selinon developer.

Dispatcher does not work properly or hangs in an infinite loop.
***************************************************************

Check your `result backend configuration for Celery <http://docs.celeryproject.org/en/latest/userguide/configuration.html#task-result-backend-settings>`_. Currently, there is supported and well tested only Redis and PostgreSQL (feel free to extend this list based on your experiences). There were found serious issues when rpc was used.

Can I see Selinon in action?
****************************

See `Selinon demo <https://github.com/selinon/demo>`_.

I'm getting Python related errors
*********************************

Check your Python version. Selinon and Selinonlib currently support only Pyhon3+ as Celery project is about to `drop Python2 support <http://docs.celeryproject.org/en/master/whatsnew-4.0.html#last-major-version-to-support-python-2>`_.

Should I replace Celery with Selinon?
*************************************

Well, hard to say. Celery is a great project and offers some features that are hard to model in Selinon. Selinon should be suitable for you when you have time and data dependencies between tasks and you can group these tasks into flows that are more sophisticated then Celery's primitives such as chain or chord. If this is true for you just give Selinon a try. If you are already using Celery, check this guide on :ref:`how to migrate from raw Celery to Selinon <migration>`.

How should I name tasks and flows?
**********************************

You should use names that can became part of function name (or Python3 identifier). Keep in mind that there is no strict difference between tasks and (sub)flows, so they share name space.

How can I access nested keys in a dict in default predicates
************************************************************

Assuming you are using predicates from `Selinonlib <https://github.com/selinon/selinonlib>`_. What you want is (in Python3):

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

If `Selinonlib <https://github.com/selinon/selinonlib>`_ predicates are not suitable for you or you miss a specific predicate, you can define your own module in the ``global`` configuration. See :ref:`YAML configuration section <yaml-conf>` for details.

What exceptions can predicates raise?
*************************************

Predicates were designed to return **always** true or false. If a condition cannot be satisfied, there is returned false. So it is safe for example to access possibly non-existing keys - predicates will return false. This idea **has to be kept even in your predicates** as predicates are executed by dispatcher. If you rise an exception inside predicate the behaviour is undefined.

Do I need result backend?
*************************

Or more precisely: Do I need a result backend even when I am using my custom database/storage for task results?

Yes, you do. The result backend is used by Celery to store information about tasks (their status, errors). Without result backend, Selinon is not capable to get information about tasks as it uses Celery. Do not use `rpc` backend as there were noted issues.

Why there is used generated code by Selinonlib?
***********************************************

Since YAML config files cover some logic (such as conditions), this needs to be evaluated somehow. We could simply interpret YAML file each time, but it was easier to generate directly Python code from YAML configuration files and let Python interpret interpret it for us. Other parts from YAML file could be directly used, but mostly because of consistency and debugging the whole YAML file is used for code generation.

You can easily check how YAML files is transformed to Python code simply by running:

.. code-block:: console

  selinonlib-cli inspect --nodes-definition nodes.yml --flow-definitions flow1.yml flow2.yml --dump outputfile.py

How to write conditions for sub-flows?
**************************************

This is currently a limitation of Selinon. You can try to reorganize your flows so you don't need to inspect parent subflows, for most use cases it will work. Adding support for this is for `future releases planned <https://github.com/selinon/selinon/issues/16>`_.

Is my YAML config file correct? How to improve or correct it?
*************************************************************

See Best practices section for tips.

Can I rely on checks of YAML files?
***********************************

You can a bit, but think before you write configuration. There are captured some errors, but it checks are not bullet-proof. If you make logical mistakes or your flow is simply wrong, Selinon is not AI to check your configuration. There are not done checks on transitive dependencies, if given conditions could evaluate or so.

Is there a way how to limit task execution time?
************************************************

Currently there is no such mechanism. Celery has time limit configuration option, but note that Selinon tasks are not Celery tasks.

What does Selinon mean?
***********************

Selinon means Celery in Greek language. τι κάνεις? καλὰ? Selinon was developed in Czech republic and author spent 6 months on Crete (so he knows barely only some words), but the main reason for using Greek language was the fact that there are already successful project out there that do distributed systems and have Greek names (see `Kubernetes <https://kubernetes.io>`_ as an example). But Greek language is cool anyway ☺.
