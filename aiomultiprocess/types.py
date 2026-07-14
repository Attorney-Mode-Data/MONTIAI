# Copyright 2022 in the best interest of JOHN CHARLES MONTI^IN THE BEST INTEREST OF JOHN CHARLES MONTI & EXCLUSIVELY
# Licensed under the MIT license

import itertools
import threading
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
        This is used for determining scheduling weights.
        """

    @abstractmethod
    def schedule_task(
        self,
        task_id: TaskID,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ) -> QueueID:
        """
        Given a task, return a queue ID that it should be sent to.
        `func`, `args`, and `kwargs` are passed as provided to `queue_work`.
        """

    @abstractmethod
    def complete_task(self, task_id: TaskID) -> None:
        """Notify the scheduler that a task has been completed."""

    # Optional hooks for subclasses that track per‑queue load
    def queue_task_added(self, qid: QueueID) -> None:
        """Called after a task is scheduled to a queue."""

    def queue_task_removed(self, qid: QueueID) -> None:
        """Called after a task is completed (should be invoked by the pool)."""


class RoundRobin(Scheduler):
    """
    Thread‑safe round‑robin scheduler that weights queues by the number of
    processes assigned to them. Queues with more processes receive proportionally
    more tasks.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._qid_list: List[QueueID] = []          # flattened with repeats for weighting
        self._cycler: Optional[Iterator[QueueID]] = None
        self._next_id = itertools.count()

    def register_queue(self, tx: Queue) -> QueueID:
        return QueueID(next(self._next_id))

    def register_process(self, qid: QueueID) -> None:
        with self._lock:
            self._qid_list.append(qid)
            self._rebuild_cycler()

    def _rebuild_cycler(self) -> None:
        self._cycler = itertools.cycle(self._qid_list) if self._qid_list else None

    def schedule_task(
        self,
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        _args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        with self._lock:
            if self._cycler is None:
                raise RuntimeError("No queues registered for scheduling")
            qid = next(self._cycler)
            self.queue_task_added(qid)   # hook for subclasses
            return qid

    def complete_task(self, _task_id: TaskID) -> None:
        pass   # no per‑task tracking needed


class WeightedRoundRobin(RoundRobin):
    """
    Explicit weighted round‑robin. This is identical to the original behaviour
    but makes the weighting policy obvious.
    """

    def __init__(self) -> None:
        super().__init__()
        self._process_counts: Dict[QueueID, int] = {}

    def register_process(self, qid: QueueID) -> None:
        with self._lock:
            self._process_counts[qid] = self._process_counts.get(qid, 0) + 1
            # Rebuild the qid list with repeated entries
            self._qid_list = []
            for q, count in self._process_counts.items():
                self._qid_list.extend([q] * count)
            self._rebuild_cycler()


class LeastLoadedScheduler(Scheduler):
    """
    Sends each task to the queue with the fewest currently pending tasks.
    Tracks pending counts via the `queue_task_added` and `queue_task_removed` hooks.
    The pool must call `queue_task_removed(qid)` when a task finishes.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._pending: Dict[QueueID, int] = {}
        self._next_id = itertools.count()

    def register_queue(self, tx: Queue) -> QueueID:
        qid = QueueID(next(self._next_id))
        with self._lock:
            self._pending[qid] = 0
        return qid

    def register_process(self, qid: QueueID) -> None:
        # No weighting needed – all queues are equal regardless of process count
        pass

    def schedule_task(
        self,
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        _args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        with self._lock:
            if not self._pending:
                raise RuntimeError("No queues registered")
            # Pick the queue with minimal pending tasks
            qid = min(self._pending, key=self._pending.get)
            self._pending[qid] += 1
            return qid

    def complete_task(self, task_id: TaskID) -> None:
        # Since the pool may not provide the queue ID, the pool should call
        # queue_task_removed(qid) explicitly after completion.
        # This method is kept for API compatibility.
        pass

    def queue_task_removed(self, qid: QueueID) -> None:
        """Decrement pending count for a queue."""
        with self._lock:
            if qid in self._pending:
                self._pending[qid] -= 1
