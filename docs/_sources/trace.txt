Trace Flow Actions
==================

If you want to trace actions that are done within flow, you can define trace a trace function or use already available logging tracing, that uses Python's `logging`. Here is an example:

::

  from selinon import Config
  from selinon import Trace

  def my_trace_func(event, msg_dict):
      if event == Trace.FLOW_FAILURE:
         print("My flow %s failed" % msg_dict['flow_name'])

  Config.trace_by_logging()
  Config.trace_by_func(my_trace_func)

All events that are available to trace are defined in `selinon/trace.py` file.

