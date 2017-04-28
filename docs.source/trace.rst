.. _trace:

Trace Flow Actions
==================

If you want to trace actions that are done within flow, you can define trace a trace function or use already available logging tracing, that uses Python's `logging`. See `trace` option in `global` section.

::

  from selinon import Trace

  def my_trace_func(event, msg_dict):
      if event == Trace.FLOW_FAILURE:
         print("My flow %s failed" % msg_dict['flow_name'])

All events that are available to trace are defined in `trace module <https://selinon.github.io/selinon/docs/api/selinon.trace.html>`_.

