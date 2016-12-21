#!/usr/bin/env python


class Retry(Exception):
    """
    Retry task as would Celery do except you can only specify countdown for retry
    """
    def __init__(self, countdown):
        self.countdown = countdown
        Exception.__init__(self, countdown)
