.. _faq:

Frequently Asked Questions (FAQ)
--------------------------------

I see only one contributor, should I trust you?
***********************************************

There is currently one contributor, but every project started somehow. Selinon was designed for `fabric8-analytics project at Red Hat <https://github.com/fabric8-analytics>`_ that gathers project information for `openshift.io <https://openshift.io>`_ (introduced at the Red Hat 2017 summit keynote), where it is still used and it already served millions flows and even more tasks. If you find Selinon interesting for your use-case, feel free to use it (and buy me some beer or at least let me know `that you like it or share any experiences you have <https://saythanks.io/to/fridex>`_).

If you find a bug, place for enhancement or anything where I can be helpful, feel free to let me know. And not to forget - even you can be :ref:`Selinon developer <development>`.

Dispatcher does not work properly or hangs in an infinite loop.
***************************************************************

Check your `result backend configuration for Celery <http://docs.celeryproject.org/en/latest/userguide/configuration.html#task-result-backend-settings>`_. Currently, there is supported and well tested only Redis and PostgreSQL (feel free to extend this list based on your experiences). There were found serious issues when rpc was used.

Can I see Selinon in action?
****************************

See `Selinon demo <https://github.com/selinon/demo>`_ or `fabric8-analytics project <https://github.com/fabric8-analytics>`_, especially it's `fabric8-analytics-worker <https://github.com/fabric8-analytics/fabric8-analytics-worker>`_.

Can I simulate Selinon run without deploying huge infrastructure?
*****************************************************************

Yes, you can. Just use shipped simulator:

.. code-block:: console

  selinonlib-cli simulate --nodes-definition nodes.yml --flow-definitions flow1.yml flow2.yml --help

This way you can also use Selinon to run your flows from a CLI. You can also explore prepared `containerized demo <http://github.com/selinon/demo>`_.

I'm getting Python related errors!
**********************************

Check your Python version. Selinon and Selinonlib currently support only Pyhon3+ as Celery project is about to `drop Python2 support <http://docs.celeryproject.org/en/master/whatsnew-4.0.html#last-major-version-to-support-python-2>`_.

Should I replace Celery with Selinon?
*************************************

Well, hard to say. Celery is a great project and offers a lot of features. Selinon should be suitable for you when you have time and data dependencies between tasks and you can group these tasks into flows that are more sophisticated than Celery's primitives such as chain or chord. If this is true for you, just give Selinon a try. If you are already using Celery, check prepared guide on :ref:`how to migrate from raw Celery to Selinon <celery>`.

How should I name tasks and flows?
**********************************

You should use names that can became part of function name (or Python3 identifier). Keep in mind that there is no strict difference between tasks, flows and sub-flows, so they share name space.

How can I access nested keys in a dict in default predicates?
*************************************************************

Assuming you are using predicates from `Selinonlib <https://github.com/selinon/selinonlib>`_. What you want is (in Python3):

.. code-block:: python

  message['foo']['bar'] == "baz"

Predicates were designed to deal with this - just provide list of keys, where position in a list describes key position:

.. code-block:: yaml

  condition:
    name: 'fieldEqual'
    args:
        key:
            - 'foo'
            - 'bar'
        value: 'baz'

I need a custom predicate, how to write it?
*******************************************

If `Selinonlib <https://github.com/selinon/selinonlib>`_ predicates are not suitable for you or you miss a specific predicate, you can define your own module in the ``global`` configuration. See :ref:`YAML configuration section <yaml>` for details.

What exceptions can predicates raise?
*************************************

Predicates were designed to return **always** true or false. If a condition cannot be satisfied, there is returned false. So it is safe for example to access possibly non-existing keys - predicates will return false. This idea **has to be kept even in your predicates** as predicates are executed by dispatcher. If you rise an exception inside predicate the behaviour is undefined.

.. danger::

  Predicates were designed to return **always** true or false. No exceptions can be raised!

Do I need result backend?
*************************

Or more precisely: Do I need a result backend even when I am using my custom database/storage for task results?

Yes, you do. The result backend is used by Celery to store information about tasks (their status, errors). Without result backend, Selinon is not capable to get information about tasks as it uses Celery. Do not use `rpc` backend as there were noted issues.

Why there is used generated code by Selinonlib?
***********************************************

Since YAML config files cover some logic (such as conditions), this needs to be evaluated somehow. We could simply interpret YAML file each time, but it was easier to generate directly Python code from YAML configuration files and let Python interpreter interpret it for us. Other parts from YAML file could be directly used, but mostly because of consistency and debugging the whole YAML file is used for code generation.

You can easily check how YAML files is transformed to Python code simply by running:

.. code-block:: console

  selinonlib-cli inspect --nodes-definition nodes.yml --flow-definitions flow1.yml flow2.yml --dump outputfile.py

How to write conditions for sub-flows?
**************************************

This is currently a limitation of Selinon. You can try to reorganize your flows so you don't need to inspect parent subflows, for most use cases it will work. Adding support for this is for `future releases planned <https://github.com/selinon/selinon/issues/16>`_.

Is it possible to do changes in the configuration and do continuous redeployment?
*********************************************************************************

Yes, you can do so. **BUT** make sure you do migrations - see the :ref:`migration section <migrations>` to get insights on how to do it properly.

What happens if I forgot to do migrations?
******************************************

If you do changes in the YAML configuration files and you do not perform migrations, unpredictable things may happen if your queues have still old messages. It's **always** a good idea to check whether migration files need to be generated. See :ref:`migrations` for more details.

Is my YAML config file correct? How to improve or correct it?
*************************************************************

See :ref:`practices` section for tips.

Can I rely on checks of YAML files?
***********************************

You can a bit, but think before you write configuration. There are captured some errors, but checks are not bullet-proof. If you make logical mistakes or your flow is simply wrong, Selinon is not AI to check your configuration. There are not done checks on transitive dependencies, if given conditions could evaluate or so.

Is there a way how to limit task execution time?
************************************************

Currently there is no such mechanism. Celery has time limit configuration option, but note that Selinon tasks are not Celery tasks.

Why there is no support for older Celery versions?
**************************************************

One of the requirements of Selinon is, that it defines tasks (:class:`Dispatcher <selinon.dispatcher.Dispatcher>` and :class:`SelinonTaskEnvelope <selinon.selinon_taskEnvelope.SelinonTaskEnvelope>`) before the Celery's application gets instantiated. Older versions of Celery requested tasks to be registered after the Celery's application was created. This makes it chicken-egg problem.

What broker type do I need?
***************************

Selinon uses Celery for queue handling and running, so you have to use broker implementation that is `supported by Celery <http://docs.celeryproject.org/en/latest/getting-started/brokers/>`_ - such as SQS or RabbitMQ.

Selinon requires that you messages are delivered - it's okay if messages are delivered more than once (see for example SQS details regarding deliver at least one). You will just end up with multiple tasks executed at the same time. You can tackle that in your application logic.

What does Selinon mean?
***********************

Selinon means Celery in Greek language. The main reason for using Greek language was the fact that there are already successful project out there that do distributed systems and have Greek names (see `Kubernetes <https://kubernetes.io>`_ as an example). But Greek language is cool anyway :-).
