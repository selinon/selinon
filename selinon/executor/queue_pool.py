#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Pool of all queues in the system."""

from .time_queue import TimeQueue


class QueuePool:
    """Pool of all queues in the system."""

    class _QueueWrapper:
        """Wrap a queue so we carry additional info needed for QueuePool - cyclic double linked list."""

        def __init__(self, previous_wrapper, next_wrapper, queue_name):
            """Init QueueWrapper.

            :param previous_wrapper: preceding queue wrapper in the linked list
            :param next_wrapper: next queue wrapper in the linked list
            :param queue_name: name of the queue that is wrapped
            """
            self.queue_name = queue_name
            self.queue = TimeQueue()
            self.previous = previous_wrapper or self
            self.next = next_wrapper or self

        def __next__(self):  # noqa
            return self.next

        def __repr__(self):
            """Queue representation for nice logs.

            :return: queue representation
            """
            return "%s(queue='%s')" % (self.__class__.__name__, self.queue_name)

    def __init__(self):
        """Initialize pool of queues."""
        # Queues are instantiated lazily on demand.
        self._queues = {}
        self._last_used = None
        self._queue_head = None
        self._queue_tail = None

    def _create_queue_wrapper(self, queue_name):
        """Create queue wrapper for the given queue name and register it to our queue pool (cyclic double-linked list).

        :param queue_name: queue name that the wrapper represents
        :return queue wrapper
        """
        queue_wrapper = self._QueueWrapper(self._queue_tail, self._queue_head, queue_name)

        if self._queue_tail is not None:
            self._queue_tail.next = queue_wrapper
        self._queue_tail = queue_wrapper

        if self._queue_head is None:
            self._queue_head = queue_wrapper
        self._queue_head.previous = queue_wrapper

        return queue_wrapper

    def _remove_queue_wrapper(self, queue_wrapper):
        """Remove queue wrapper from queue pool - double linked list."""
        self._queues.pop(queue_wrapper.queue_name)

        if queue_wrapper is self._queue_head and queue_wrapper is self._queue_tail:
            self._queue_head = None
            self._queue_tail = None
            self._last_used = None
            return

        queue_wrapper.previous.next = queue_wrapper.next
        queue_wrapper.next.previous = queue_wrapper.previous

        if queue_wrapper is self._queue_head:
            self._queue_head = queue_wrapper.next

        if queue_wrapper is self._queue_tail:
            self._queue_tail = queue_wrapper.previous

        if queue_wrapper is self._last_used:
            self._last_used = queue_wrapper.previous

    def get_queue(self, name):
        """Get queue wrapper by name of the queue that is wrapped, if does not exist, create one lazily.

        :param name: a name of the queue
        :return: queue wrapper for the given queue with requested name
        """
        queue_wrapper = self._queues.get(name)
        if queue_wrapper is None:
            queue_wrapper = self._create_queue_wrapper(name)
            self._queues[name] = queue_wrapper
        return queue_wrapper

    def queue_exists(self, name):
        """Check whether a queue with the given name exists.

        :param name: name of queue to be checked if exists
        :return: True if such queue exists in queue pool
        """
        return name in self._queues

    def push(self, queue_name, time, record):
        """Push record with its time to queue with name queue_name.

        :param queue_name: a queue name that should keep record
        :param time: time of record (when should be record executed)
        :param record: record itself (message with additional information such as task name and its parameters)
        """
        # TODO: add lock per queue
        queue_wrapper = self.get_queue(queue_name)
        queue_wrapper.queue.push(time, record)

        # initialize head if we haven't pushed anything to queue yet
        # so we start from the very first queue
        if self._last_used is None:
            self._last_used = queue_wrapper

    def pop(self):
        """Pop a record with the smallest time.

        :return: (time, record) tuple -  time of record and record itself (see self.push for more info)
        """
        # TODO: add lock per queue
        assert self._last_used is not None  # nosec

        exit_loop = False
        result_time, result_record = self._last_used.queue.top()
        result_queue_wrapper = self._last_used
        queue_wrapper = next(self._last_used)

        while not exit_loop:
            candidate_time, candidate_record = queue_wrapper.queue.top()

            if result_time is None or candidate_time < result_time:
                result_time, result_record = candidate_time, candidate_record
                result_queue_wrapper = queue_wrapper

            queue_wrapper = next(queue_wrapper)
            if queue_wrapper == self._last_used:
                exit_loop = True

        result_queue_wrapper.queue.pop()

        if result_queue_wrapper.queue.is_empty():
            self._remove_queue_wrapper(result_queue_wrapper)
        else:
            self._last_used = result_queue_wrapper

        return result_time, result_record

    def is_empty(self):
        """Check pool emptiness.

        :return: True if queue pool does not have any messages and no queues (as queues get deleted when empty)
        """
        return len(self._queues) == 0

    def __repr__(self):
        """Queue pool representation.

        :return: a string representing pool (with a list of active queues)
        """
        return "%s(%s)" % (self.__class__.__name__, self._queues)
