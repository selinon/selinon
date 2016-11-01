# Selinon
Python Celery dispatcher worker for dynamically scheduling tasks

![codecov](https://codecov.io/gh/selinon/selinon/branch/master/graph/badge.svg)](https://codecov.io/gh/selinon/selinon)
![PyPI Current Version](https://img.shields.io/pypi/v/selinon.svg)
![PyPI Implementation](https://img.shields.io/pypi/implementation/selinon.svg)
![PyPI Wheel](https://img.shields.io/pypi/wheel/selinon.svg)
![Travis CI](https://travis-ci.org/selinon/selinon.svg?branch=master)
![GitHub stars](https://img.shields.io/github/stars/selinon/selinon.svg)
![GitHub license](https://img.shields.io/badge/license-GPLv2-blue.svg)
![Twitter](https://img.shields.io/twitter/url/http/github.com/selinon/selinon.svg?style=social)

## TLDR;

Advanced flow management above Celery written in Python3, that allows you:

  - Dynamically schedule tasks based on results of previous tasks
  - Group tasks to flows
  - Schedule flows from other flows (even recursively)
  - Store results of tasks in defined storages transparently, validate results against defined JSON schemas
  - Track flow progress via the build-in tracing mechanism
  - Complex per-task or per-flow failure handling with fallback tasks
  - And (of course) much more...

## About

This tool is an implementation above Celery that enables you to define flows and dependencies in flows, schedule tasks based on results of Celery workers, their success or any external events.

Imagine you have a distributed task queue you want to run. [Celery](http://www.celeryproject.org/) lets you define execution units called tasks, handles scheduling of tasks but does not let you define flows except primitive ones such as [chord or chain](http://docs.celeryproject.org/en/latest/userguide/canvas.html). If you want to build more sophisticated solutions you could be highly limited with those primitives.

## More info

See [Documentation](https://selinon.github.io/selinon) for examples, documentation and more info about project.

## Installation

```
$ pip3 install selinon
```


