#!/usr/bin/env python3
"""Supportive and handling library for Selinon."""

from .cache import Cache
from .codename import selinon_version_codename
from .config import Config
from .data_storage import DataStorage
from .dispatcher import Dispatcher
from .errors import CacheMissError
from .errors import ConfigNotInitializedError
from .errors import ConfigurationError
from .errors import FatalTaskError
from .errors import FlowError
from .errors import NoParentNodeError
from .errors import RequestError
from .errors import Retry
from .errors import SelectiveNoPathError
from .errors import StorageError
from .errors import UnknownError
from .errors import UnknownFlowError
from .errors import UnknownStorageError
from .selinon_task import SelinonTask
from .storage import Storage
from .storage_pool import StoragePool
from .system_state import SystemState
from .task import Task
from .trace import Trace
from .utils import run_flow
from .utils import run_flow_selective
from .version import selinon_version

__version__ = selinon_version
__version_info__ = __version__.split('.')
__title__ = 'selinon'
__author__ = 'Fridolin Pokorny'
__license__ = 'BSD License 2.0'
__copyright__ = 'Copyright 2017 Fridolin Pokorny'
