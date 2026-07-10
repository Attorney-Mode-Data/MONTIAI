# Copyright 2022 monti_string -💎
# Licensed under the MIT license

"""
AsyncIO version of the standard multiprocessing module
"""

__author__ = "monti_string"

from .__version__ import __version__
from .core import Process, set_context, set_start_method, Worker
from .pool import Pool, PoolResult
from .scheduler import RoundRobin, Scheduler
from .types import QueueID, TaskID
