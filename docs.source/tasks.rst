Task Implementation
===================

A task is of type CeleriacTask. Constructor of task is transparently called by CeleriacTaskEnvelope, which handles arguments propagation, parent node propagation. Base class CeleriacTask also handles how data should be retrieved from database in case you want to retrieve data from parent tasks.

The only thing you need to define is `args` parameter based on which Task computes its results. The return value of your Task is than checked against JSON schema and stored to database if configured so.

::

  from celeriac import CeleriacTask

  class MyTask(CeleriacTask):
     def execute(self, args):
     pass

In order to retrieve data from parent task, one can call `self.parent_result(task_name, task_id)`. Results of parent nodes that are flows are propagated only if `propagate_finished` was set. In that case parent key is a name of flow and consists of dictionary containing task names run that were run as a keys and and list of task ids as value. Any subflow run within flow is hidden and results of tasks are retrieved recursively.
