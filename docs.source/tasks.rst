Task Implementation
===================

A task is of type `SelinonTask`. Constructor of task is transparently called by SelinonTaskEnvelope, which handles arguments propagation, parent node propagation. Base class SelinonTask also handles how data should be retrieved from database in case you want to retrieve data from parent tasks.

The only thing you need to define is `node_args` parameter based on which Task computes its results. The return value of your Task is than checked against JSON schema and stored to database if configured so.

::

  from selinon import SelinonTask

  class MyTask(SelinonTask):
     def run(self, node_args):
       pass

In order to retrieve data from parent task/flow, one can call `self.parent_task_result()` or `self.parent_flow_result()` (see `sources <https://fridex.github.io/selinon/api/selinon.selinonTask.html>`_). Results of parent nodes that are flows are propagated only if `propagate_finished` was set. In that case parent key is a name of flow and consists of dictionary containing task names run that were run as a keys and and list of task ids as value.

If your task should not be rescheduled due to a fatal error, raise `FatalTaskError`. This will cause fatal task error and task will not be rescheduled . Keep in mind that `max_retry` from YAML configuration file will be ignored).

In case you want to reschedule your task without affecting max_retry, just call `self.retry()`. Optional argument `countdown` specifies countdown for rescheduling.
