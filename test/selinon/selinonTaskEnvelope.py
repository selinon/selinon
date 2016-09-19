#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

from .config import Config


class SelinonTaskEnvelope(object):
    """
    A base class for user defined workers
    """
    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    name = "SelinonTask"

    @classmethod
    def apply_async(cls, kwargs, queue):
        # pop queue since it is selinon specific
        task = Config.get_task_instance(**kwargs)
        # queue used only for testing
        task.queue = queue
        return task

