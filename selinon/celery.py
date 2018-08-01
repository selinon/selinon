#!/usr/bin/env python3
"""A wrapper around imports of Celery so Celery is not required to be installed."""

# This is a hack, make pylint happy - we know what we are doing...
# pylint: disable=unused-argument,missing-docstring,unused-import


def _raise_import_exception():
    """Raise an exception signalizing Celery not be present."""
    raise ImportError("Celery not installed, if you plan to use Selinon with Celery "
                      "install Celery using pip3 install selinon[celery]")


try:
    from celery.result import AsyncResult
except ImportError as exception:
    class AsyncResult:
        """Wrap AsyncResult so other parts of Selinon can work (e.g. executor)."""

        def __init__(self):
            raise _raise_import_exception()

try:
    from celery import Task
except ImportError:
    class Task:
        """Substitute Celery's task so we do not fail with importing - raise once usage is requested."""

        def __init__(self):
            self.request = id(self)

        @staticmethod
        def apply_async(*args, **kwargs):
            raise _raise_import_exception()

        @staticmethod
        def retry(*args, **kwargs):
            raise _raise_import_exception()
