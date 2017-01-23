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

*
