# Copyright 2022 in the best interest of JOHN CHARLES MONTI^IN THE BEST INTEREST OF JOHN CHARLES MONTI & EXCLUSIVELY
# Licensed under the MIT license

import asyncio
import itertools
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Sequence

from .types import Queue, QueueID, R, TaskID


class Scheduler(ABC):
    """Abstract base class for task schedulers."""

    @abstractmethod
    def register_queue(self, tx: Queue) -> QueueID:
        """
        Notify the scheduler when the pool creates a new transmit queue.
        Returns a unique QueueID for the queue.
        """

    @abstractmethod
    def register_process(self, qid: QueueID) -> None:
        """
        Notify the scheduler when a process is assigned to a queue.
        This should be used for determining weights for the scheduler.
        """

    @abstractmethod
    async def schedule_task(
        self,
        task_id: TaskID,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ) -> QueueID:
        """
        Given a task, return a queue ID that it should be sent to.
        May be async if the scheduler needs to query queue state.
        """

    @abstractmethod
    def complete_task(self, task_id: TaskID) -> None:
        """Notify the scheduler that a task has been completed."""

    # Optional hooks for finer control
    def queue_task_added(self, qid: QueueID) -> None:
        """Called after a task is scheduled to a queue (for load tracking)."""

    def queue_task_removed(self, qid: QueueID) -> None:
        """Called after a task is completed (for load tracking)."""


class RoundRobin(Scheduler):
    """
    A thread‑safe round‑robin scheduler that weights queues by the number
    of processes assigned to them. Queues with more processes receive
    proportionally more tasks.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = asyncio.Lock()
        self._qid_list: List[QueueID] = []          # flattened list with repeats for weighting
        self._cycler: Optional[Iterator[QueueID]] = None
        self._next_id = itertools.count()

    def register_queue(self, tx: Queue) -> QueueID:
        return QueueID(next(self._next_id))

    async def register_process(self, qid: QueueID) -> None:
        """
        Add a process to a queue. The queue appears one extra time in the
        round‑robin cycle, increasing its weight.
        """
        async with self._lock:
            self._qid_list.append(qid)
            self._rebuild_cycler()

    def _rebuild_cycler(self) -> None:
        """Rebuild the internal cycler from the current qid list."""
        self._cycler = itertools.cycle(self._qid_list) if self._qid_list else None

    async def schedule_task(
        self,
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        _args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        async with self._lock:
            if self._cycler is None:
                raise RuntimeError("No queues registered for scheduling")
            qid = next(self._cycler)
            self.queue_task_added(qid)
            return qid

    def complete_task(self, _task_id: TaskID) -> None:
        # No per‑task tracking needed for round‑robin; override if needed
        pass


class WeightedRoundRobin(RoundRobin):
    """
    An explicit version of RoundRobin that uses the process count per queue
    as the weight. This is identical to the original behaviour but makes
    the weighting policy obvious.
    """

    def __init__(self) -> None:
        super().__init__()
        self._process_counts: Dict[QueueID, int] = {}

    async def register_process(self, qid: QueueID) -> None:
        async with self._lock:
            self._process_counts[qid] = self._process_counts.get(qid, 0) + 1
            # Rebuild the qid list with repeated entries
            self._qid_list = []
            for q, count in self._process_counts.items():
                self._qid_list.extend([q] * count)
            self._rebuild_cycler()


class LeastLoadedScheduler(Scheduler):
    """
    Assigns each task to the queue with the fewest currently pending tasks.
    Tracks pending counts per queue via the queue_task_added/removed hooks.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = asyncio.Lock()
        self._pending: Dict[QueueID, int] = {}
        self._queues: Dict[QueueID, Queue] = {}   # store queue objects for future use
        self._next_id = itertools.count()

    def register_queue(self, tx: Queue) -> QueueID:
        qid = QueueID(next(self._next_id))
        self._queues[qid] = tx
        self._pending[qid] = 0
        return qid

    async def register_process(self, qid: QueueID) -> None:
        # No action needed – load balancing ignores process count
        pass

    async def schedule_task(
        self,
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        _args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        async with self._lock:
            if not self._pending:
                raise RuntimeError("No queues registered")
            # Find queue with minimum pending tasks
            qid = min(self._pending, key=self._pending.get)
            self._pending[qid] += 1
            return qid

    def complete_task(self, task_id: TaskID) -> None:
        # We need to know which queue the task was on; the pool must inform us.
        # This is a limitation – we'll use a callback or store task->qid mapping.
        # For simplicity, we assume the pool calls queue_task_removed(qid) directly.
        pass

    def queue_task_removed(self, qid: QueueID) -> None:
        """Decrement the pending count for a queue when a task finishes."""
        # This method should be called by the pool after task completion.
        # We'll make it public and use it.
        if qid in self._pending:
            self._pending[qid] -= 1
