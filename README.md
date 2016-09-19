# Selinon
Python Celery dispatcher worker for dynamically scheduling tasks

## About

This tool is an implementation above Celery that enables you to define flows and dependencies in flows, schedule tasks based on results of Celery workers, their success or any external events.

Imagine you have a distributed task queue you want to run. [Celery](http://www.celeryproject.org/) lets you define execution units called tasks, handles scheduling of tasks but does not let you define flows except primitive ones such as [chord or chain](http://docs.celeryproject.org/en/latest/userguide/canvas.html). If you want to build more sophisticated solutions you could be highly limited with those primitives.

## More info

See [Documentation](https://fridex.github.io/selinon) for examples, documentation and more info about project.


