# Celeriac - Dynamic Task Flow Management on top of Celery

## What is Celeriac and why should I use it?

Celeriac is a tool that gives you a power to define flows, subflows of tasks that should be executed in Celery - a distributed task queue. If you want to define very simple flows, Celery offers you workflow primitives that can be used. Unfortunately these workflow primitives are very limited when it comes to extending your workflow, working with multiple dependencies or scheduling tasks based on particular conditions.

If you are looking for a workflow solution, take a look at existing solutions, such as Azkaban, Airflow, Nifi, Luigi and many others. The main advantage of using Celeriac over these is the fact, that you can use it in fully distributed systems and for example let Kubernetes do the workload (and much more, such as recursive flows, subflows support, ...).

## Is there available a demo?

You can take a look at celeriac-demo so you can see how Celeriac works without deep diving into configuration. Just run `docker-compose up`.

## What do I need?

In order to use Celeriac, you need:

  * a basic knowledge of Python and YAML language
  * a basic knowledge of Celery and its configuration
  * vegetable appetite :)

## How does it work? - a high level overview

The key idea lies in Dispatcher - there is created Dispatcher Celery task for each flow. Dispatcher takes care of starting new tasks and subflows, checking their results and scheduling new tasks based on your configuration.

## YAML Configuration

The only thing you have to provide is a configuration file that describes how should be your flow organized. A flow is defined by nodes that can be either tasks or another flows (subflows). Once you create a YAML configuration file, this file is parsed and checked using Parsley tool (you can even visualize your flows based on configuration). Parsley automatically generates Python code for Dispatcher to let Dispatcher know what to do when.


Now let's a look at YAML configuration file structure. At the top level, there are listed following keys:
```yaml
---
  tasks:
    # a list of tasks available within the system
  flows:
    # a list of flows available within the system
  storages:
    # a list of storages available within the system
  flow-definitions:
    # a list of flow definitions
```


## Entities in System

There are two main entities in the system:

  * tasks
  * storages
  
Tasks are organized into flows based on dependencies defined by you in your configuration file. Each flow can be used as a subflow and can become a part of another flow. This way you can see tasks as nodes in dependency graph and flows as nodes as well.
  
### Tasks

Configuration of a task in configuration can look like the following example.

```yaml
 tasks:
    - name: 'MyTask1'
      classname: 'MyTask1Class'
      import: 'myproject.workers'
      max_retry: 5
      retry_countdown: 120
      output_schema: 'myproject/schemas/my-task1.json'
      storage: "Storage1"
```

A task definition has to be placed into `tasks` section, which consists of list of task definitions.

A list of all configuration options:

###### `name`
A name of a task. This name is used to refer task in your flows, it is not necessarily task's class name (see `classname` option).

 * Possible types:
   * string
  
 * Required: True
  
###### `import`
Module that should be imported in order to use a task.

 * Possible types:
   * string
  
 * Required: True

###### `classname`
Name of a class that should be imported. If omitted it defaults to `name`. Task `MyTask1` from example is then imported as:
```python
from myproject.workers import MyTask1Class
# if classname is omitted:
#from myproject.workers import MyTask1
```

 * Possible types:
   * string
 * Required: False

Default:
  defaults to task's `name`

###### `max_retry`
Maximum number of retries of the task before the task is marked as *failed*. See fallbacks section for more info.

 * Possible types:
   * nonzero integer
  
 * Required: False

Default:
  0 - a task is run only once without retry

###### `retry_countdown`
Number of seconds before a task should be retried.

 * Possible types:
   * positive integer or positive float, can be set to zero
  
 * Required: False
  
Default:
  zero


###### `output_schema`
JSON output schema that should be used to validate results before they are stored in database. If task's result does not correspond to JSON schema, task fails and is marked as failed or retried based on `max_retry` configuration option.

 * Possible types:
   * string - path to JSON schema

 * Required: False
  
Default:
  None

###### `storage`
Storage name that should be used for task results; see Storages section.

 * Possible types:
   * string - name of storage

 * Required: False
  
Default:
  None
  
  
## Storages

Here is an example of storage configuration:

```yaml
  storages:
    - name: "Storage1"
      import: "celeriac.storage"
      classname: "SqlStorage"
      configuration:
        connection_string: 'postgresql://postgres:postgres@localhost:5432/mydatabase'
        echo: false
```
 
A storage definition has to be placed into `storages` section, which is a list of storage definitions.

A list of all configuration options:

###### `name`
A name of a storage. This name is used to refer storage in tasks.

 * Possible types:
   * string - name of storage
  
 * Required: True

###### `import`
Module that holds storage class definition.

 * Possible types:
   * string - import

 * Required: True

###### `classname`
A name of a database storage adapter class in `import` module. The class from example is imported as:
```python
from celeriac.storage import SqlStorage
# if classname is omitted
#from celeriac.storage improt Storage1
```

 * Possible types:
   * string

 * Required: False

Default:
storage `name`

###### `configuration`
Configuration that will be passed to storage adapter instance. This option depends on database adapter implementation, see Storage Implementation section.

## Flows

As stated above, a flow can become a node in dependency graph. This means that you can reuse a flow across multiple flows - flow can become a subflow.

### Flow definition

A flow definition is placed into list of flow definitions in YAML configuration file.
```yaml
flow-definitions:
  - name: "myFirstFlow"
    propagate_parent:
      - 'subflow1'
    propagate_finished:
      - 'subflow1'
    propagate_node_args:
      - 'subflow1'
    nowait:
     - 'Task1'
    edges:
      - from:
          - 'InitTask'
        to:
          - 'Task1'
          - 'subflow1'
        condition:
          name: "alwaysTrue"
        failures:
          nodes:
            - 'InitTask'
          fallback:
            - 'InitFallbackTask'
      - from:
        to: 'InitTask1'
        
  - name: 'subflow1'
    from:
    to: 'AnotherTask'
    condition:
      name: "alwaysTrue"
```

Configuration options:

###### `name`
A name of flow. This name is used to refer flow.

 * Possible types:
   * string
  
 * Required: True

###### `propagate_parent`
Propagate parent nodes to subflow or subflows, see task implementation for more details.

 * Possible types:
   * string - a name of flow to which parent nodes should be propagated
   * list of strings - a list of flow names to which parent nodes should be propagated
   * boolean - enable or disable parent nodes propagation to all subflows
  
 * Required: False
  
Default: False - do not propagate parent to any subflow

###### `propagate_finished`
Propagate finished node ids from subflows. Finished nodes from subflows will be passed as dictionary in parent dict. All task ids will be recursively received from all subflows of inspected flow. See task implementation for more details.

 * Possible types:
   * string - a name of flow from which finished should be propagated
   * list of strings - a list of flow names from which finished nodes should be propagated
   * boolean - enable or disable finished nodes propagation from all subflows
  
 * Required: False
  
Default: False - do not propagate finished from any subflow

###### `propagate_node_args`
Propagate node arguments to subflows.

 * Possible types:
   * string - a name of flow to which node arguments should be propagated
   * list of strings - a list of flow names to which node arguments should be propagated
   * boolean - enable or disable node arguments propagation to all subflows
  
 * Required: False
  
Default: False - do not propagate flow arguments to any subflow

###### `nowait`
Do not wait for node to finish. This node cannot be stated as a dependency in YAML configuration file. Note that node failure will not be tracked if marked as nowait.

 * Possible types:
   * string - a node that should be started with nowait flag
   * list of strings - a list of nodes that should be started with nowait flag
  
 * Required: False
  
Default: False - wait for all nodes to complete

###### `edges`
A list of edges describing dependency on nodes. See Edge Definition in a Flow section.

 * Possible types:
   * list of edge definition
  
 * Required: True

#### Edges Definition in a Flow
###### `from`

 * Possible types:
   * string
   * list of strings
   * None
  
 * Required: True
  
###### `to`

 * Possible types:
   * string
   * list of strings
   * boolean
  
 * Required: True

###### `condition`
A condition made of predicates. Boolean operators `and`, `or` and not can be used as desired. See Condition Definition section for more info.

 * Possible types:
   * condition definition
  
 * Required: True
  
###### `failures`
A list of failures that can occur in the system and their fallbacks. See Failures and Fallback section for more info.

 * Possible types:
   * list of failures
  
 * Required: False
  
Default: None

##### Conditions and Predicates

You can start a node based on particular conditions that needs to be met. These conditions can be either external (e.g. availability of a remote server) or flow specific (e.g. results of tasks, arguments that are passed to flow, etc.). A list of all predicates can be found in Parsley tool in `parsley.predicates` module, which is also the default module to be used for predicates.

If you would like to use your own predicates, just state `predicate_module` in your YAML configuration file on top level.

###### `predicate_module`
Use a custom predicate module.

 * Possible types:
   * string - predicate module import
  
 * Required: False
  
Default: 'parsley.predicates'

All predicates tend to be safe - they do not raise any exception. This would cause fatal error to flow. Instead they return either `True` or `False`. Nothing in-between. That means that if desired condition cannot be satisfied (e.g. requested key in result is not present), `False` is returned.

More complex boolean conditions can be created using build-in support for boolean operators `and`, `or` and `not`. Operators `and` and `or` are n-ary boolean operators (they accept a list of predicates that need to be evaluated, short circuit evaluation is applied). Logical operator `not` is unary.

A condition can look like the following example:

```
condition:
  name: "fieldEqual"
  node: "task1"
  args:
    key:
      - 'foo'
      - 'bar'
    value: 'baz'
```

###### `name`
A name of predicate that should be used in condition.

 * Possible types:
   * string - predicate name
  
 * Required: True

###### `node`
A node name that is inspected in the condition. The node has to participate on flow - has to be stated as a dependency node. This flag is required only if predicate requires results of particular node.

 * Possible types:
   * string - a node name
  
 * Required:
  False if predicate does not require a result of task or there is dependency on a single task
  True if condition is evaluated on multiple dependent nodes and predicate expects node results

Default:
  None if predicate does not require a result of task.
  If there is only dependency on a single node stated in `from`, node is automatically computed.

###### `args`
Arguments to predicate that should be passed. These arguments are dependent on used predicate - see parsley.predicates for list of all predicates available.

Predicates were designed to use "listed keys" as shown in the example - if a list of keys is provided, these keys are deferred as one would intuitively expect. For example the condition listed above will be roughly translated (without exception checks):
```python
result['foo']['bar'] == 'baz'
```

##### Failures and Fallback
  
You can define a fallback that should be run if there is a failure in your flow. There is stated a failure definition:

```yaml
  failures:
    - nodes:
        - 'Task1'
        - 'Task2'
      fallback:
        - 'FallbackTask'
```

You can specify multiple fallbacks in your flow based on nodes failure. The highest priority for Dispatcher is to succeed with the flow. Thus if you define nodes that can fail, here is how Dispatcher is trying to recover from a failure:

  * Fallbacks are run once there are no active nodes in the flow - Dispatcher is trying to recover from failures in this place.
  * There is scheduled one fallback at the time - this prevents from time dependency in failures
  * There is always chosen failure based how many nodes you expect to fail - Dispatcher is greedy with fallback - that means it always choose failure that is dependent on highest number of nodes. If multiple failures can be chosen, lexical order of node names comes in place.
  * A flow fails if there is still a node that failed and there is no failure specified to recover from failure.
  * Fallback on fallback is fully supported (and nested as desired).


###### `nodes`
Describes fallback dependency on node or nodes. Fallback is run if all nodes in listed in `nodes` failed and there is no failure that can be run before defined fallback.

 * Possible types:
   * string - a node name that triggers fallback
   * list of strings - list of node names that are trigger fallback
  
 * Required: True

###### `fallback`
Fallback that should be applied on failure.

 * Possible types:
   * string - a node name that will be run on failure
   * list of strings - list of names of nodes that will be run in case of failure
   * true - if failure should be ignored, no node is run, but failure is not treated as fatal
  
 * Required: True

### Storage Implementation

Currently, there are available three database adapters, see 'celeriac.storage' module:

  * SqlStorage - SQLAlchemy adapter for SQL databases
    Example:
    ```yaml
    storages:
      - name: 'MySqlStorage'
        classname: 'SqlStorage'
        import: 'celeriac.storage'
        configuration:
          connection_string: 'postgres://postgres:postgres@postgres:5432/postgres'
          encoding: 'utf-8'
          echo: false
    ```
    
  * RedisStorage - Redis database adapter
    Example:
    ```yaml
    storages:
      - name: 'MyRedisStorage'
        classname: 'RedisStorage'
        import: 'celeriac.storage'
        configuration:
          host: 'redishost'
          port: 6379
          db: 0
          password: 'secretpassword'
          charset: 'utf-8'
          host: 'mongohost'
          port: 27017
    ```
    
  * MongoStorage - MongoDB database adapter
    Example:
    ```yaml
    storages:
      - name: 'MyMongoStorage'
        classname: 'MongoStorage'
        import: 'celeriac.storage'
        configuration:
          db_name: 'database_name'
          collection_name: 'collection_name'
          host: 'mongohost'
          port: 27017
    ```
    
You can define your own storage by inheriting from `DataStorage` defined in `celeriac.storage`:
```python
from celeriac.storage import DataStorage

class MyStorage(DataStorage):
    def __init__(self, host, port):
        # arguments from YAML file are pasased to constructor as key-value arguments
        pass
        
    def is_connected():
        # predicate that is used to connect to database
        return False
        
    def connect():
        # define how to connect based on your configuration
        pass
        
    def disconnect():
        # define how to disconnect from database
        pass
        
    def retrieve(self, flow_name, task_name, task_id):
        # define how to retrieve result based on flow, task name and task id
        pass
        
    def store(self, flow_name, task_name, task_id, result):
        # define how to store result from task with id task_id based on flow and task name
        pass
```

You can also reuse current implementation of a storage in order to  define your custom `retrieve()` and `store()` methods based on your requirements.

#### Database Connection Pool

Each Celery worker is trying to be efficient when it comes to number of connections to a database. There is held only one instance of `DataStorage` class per whole worker. This means that your database has to be concurrency-safe if you plan to run your Celery worker with concurrency level bigger than one.

You can simply share connection across multiple `DataStorage` classes in inheritance hierarchy and reuse already defined connections.

If you would like to limit number of connections to database, you have to do it on your own by sharing connection information in parent of type `DataStorage` and implement connection limitation logic in your database adapter. This is not possible on Celeriac level, since database adapters are black box for Celeriac and they can share connection across multiple instances.

### Task Implementation

A task is of type CeleriacTask. Constructor of task is transparently called by CeleriacTaskEnvelope, which handles arguments propagation, parent node propagation. Base class CeleriacTask also handles how data should be retrieved from database in case you want to retrieve data from parent tasks.

The only thing you need to define is `args` parameter based on which Task computes its results. The return value of your Task is than checked against JSON schema and stored to database if configured so.

```python
from celeriac import CeleriacTask

class MyTask(CeleriacTask):
   def execute(self, args):
     pass
```

In order to retrieve data from parent task, one can call `self.parent_result(task_name, task_id)`. Results of parent nodes that are flows are propagated only if `propagate_finished` was set. In that case parent key is a name of flow and consists of dictionary containing task names run that were run as a keys and and list of task ids as value. Any subflow run within flow is hidden and results of tasks are retrieved recursively.

### Organization of configuration in YAML configuration file

If you have a lot of flows or you want to combine flows in different way, you can place configuration of entities (`tasks`, `storages` and `flows`) into one file (called `nodes.yaml`) and flow definitions can be split into separate files.

### Trace Flow Actions

If you want to trace actions that are done within flow, you can define trace a trace function or use already available logging tracing, that uses Python's `logging`. Here is an example:

```python
from celeriac import Config
from celeriac import Trace

def my_trace_func(event, msg_dict):
    if event == Trace.FLOW_FAILURE:
       print("My flow %s failed" % msg_dict['flow_name'])

Config.trace_by_logging()
Config.trace_by_func(my_trace_func)
```

All events that are available to trace are defined in `celeriac/trace.py` file.

### Useful Flow Patterns

This section gives you an overview if flow patterns that can be suitable for your flow.

#### Permutations of all Parent Tasks - Cyclic Diamond with Permutations

Consider following flow definition (tasks definition omitted, but all Task2 and Task3 use storage, Task4 is reading parent results):

```
```

A visualization of such flow using Parsley tool would be:

    Task1
    
  Task2 Task3

    Task4

As one would expect Task4 is called after Task2 and Task3 are finished. Consider task1_1 is task id of Task1 after it's first run, task2_1 is tasks id of Task2 after it's first, task2_2 is task id after it's second run and so forth. After the second run of Task1, Task2 and Task3 (before Task4 execution) we would have following task ids:

task1_1
task1_2
task2_1
task2_2
task3_1
task3_2
task4_1

Now we Task4 is going to be run. In the first run, it was run with parent:
```python
{'Task2': 'task2_1', 'Task3': 'task3_1'
```
Now Task4 will be run three times with parent 
```python
{'Task2': 'task2_1', 'Task3': 'task3_2'}
```

```python
{'Task2': 'task2_2', 'Task3': 'task3_1'}
```

```python
{'Task2': 'task2_2', 'Task3': 'task3_2'}
```
In other words, task Task4 will be run with all permutations of Task2 and Task3. If this is not what you want, take a look at diamond without permutations pattern listed bellow.

#### Cyclic Diamond without Permutations

If you would like to run Task4 with parent:
```python
{'Task2': 'task2_1', 'Task3': 'task3_1'}
```
 
```python
{'Task2': 'task2_2', 'Task3': 'task3_2'}
```
you have to run Task2 and Task3 in a separate flow that is marked with `propagate_finished` (and `propagate_node_args` if needed).

#### Task Aggregator

A task's parent is a dictionary a task id under task name key of parent and a dictionary of finished nodes if parent is a flow. An example of parent dictionary is:

```python
{
  'Task1': 'task1'
  ...
  'flow1': {
    'Task2': ['task2_1', 'task2_2']
    ...
  }
}
```

If you would like to aggregate results of all tasks of a type in another task (e.g. if they are computed recursively), just run task in a separate flow. After a flow finishes, you will get list of task ids under `parent['flow']['Task1']`.

#### Flow Failures versus Task Failures

You can define failures based on nodes. This means that you can define failure on a task-level or on a flow-level. If you would like to recover from flow failure, encapsulate your flow into a another flow and run fallback from it. The fallback's parent are all parents that succeeded in parent flow, so you have available results of tasks that succeeded and wrote results to database (if configured so).

If you would like to recover on task level inside flow, it would be easier for you to handle failures on task level.

