YAML Configuration
==================

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


Entities in System
##################

There are two main entities in the system:

  * tasks
  * storages
  
Tasks are organized into flows based on dependencies defined by you in your configuration file. Each flow can be used as a subflow and can become a part of another flow. This way you can see tasks as nodes in dependency graph and flows as nodes as well.
  
Tasks
#####

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
  
  
Storages
########

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

## Organization of configuration in YAML configuration file

If you have a lot of flows or you want to combine flows in different way, you can place configuration of entities (`tasks`, `storages` and `flows`) into one file (called `nodes.yaml`) and flow definitions can be split into separate files.
