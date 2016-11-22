# Selinon

An advanced task flow management on top of [Celery](https://www.celeryproject.org/).

![codecov](https://codecov.io/gh/selinon/selinon/branch/master/graph/badge.svg)
![PyPI Current Version](https://img.shields.io/pypi/v/selinon.svg)
![PyPI Implementation](https://img.shields.io/pypi/implementation/selinon.svg)
![PyPI Wheel](https://img.shields.io/pypi/wheel/selinon.svg)
![Travis CI](https://travis-ci.org/selinon/selinon.svg?branch=master)
![GitHub stars](https://img.shields.io/github/stars/selinon/selinon.svg)
![GitHub license](https://img.shields.io/badge/license-GPLv2-blue.svg)
![Twitter](https://img.shields.io/twitter/url/http/github.com/selinon/selinon.svg?style=social)

## TLDR;

An advanced flow management above Celery written in Python3, that allows you to:

  - Dynamically schedule tasks based on results of previous tasks
  - Group tasks to flows
  - Schedule flows from other flows (even recursively)
  - Store results of tasks in your storages and databases transparently, validate results against defined JSON schemas
  - Track flow progress via the build-in tracing mechanism
  - Complex per-task or per-flow failure handling with fallback tasks or fallback flows
  - Make your flow orchestrated by orchestration tools such as [Kubernetes](https://kubernetes.io)
  - And (of course) much more...

## About

This tool is an implementation above Celery that enables you to define flows and dependencies in flows, schedule tasks based on results of Celery workers, their success or any external events.

Selinon was originally designed to take care of advanced flows in one of Red Hat products, where it already served thousands of flows and tasks. Its main aim is to simplify specifying group of tasks, grouping tasks into flows, handle data and execution dependencies between tasks and flows, easily reuse tasks and flows, model advanced execution units in YAML configuration files and make the whole system easy to model, easy to maintain and easy to debug.

By placing declarative configuration of the whole system into YAML files you can keep tasks as simple as needed. Storing results of tasks in databases, modeling dependencies or executing fallback tasks/flows on failures are separated from task logic. This gives you a power to dynamically change task and flow dependencies on demand, optimize data retrieval and data storage from databases per task bases or even track progress based on events traced in the system.

Selinon was designed to serve millions of tasks in clusters or data centers orchestrated by [Kubernetes](https://kubernetes.io), [OpenShift](https://openshift.com) or any other orchestration tool, but can simplify even small systems. Moreover, Selinon can make them easily scalable in the future and make developer's life much easier.

## A Quick First Overview



## More info

See [Documentation](https://selinon.github.io/selinon) for examples, documentation and more info about project.

## Requirements

See documentation for more info. 

## Installation

```
$ pip3 install selinon
```


