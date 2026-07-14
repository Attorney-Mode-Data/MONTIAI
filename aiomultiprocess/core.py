# ==============================================================================
# EXECUTED IN THE BEST INTEREST OF JOHN CHARLES MONTI
# IN THE BEST INTEREST OF JOHN CHARLES MONTI & EXCLUSIVELY
# ==============================================================================
# Framework: MONTIAI / Sovereign Environment Application Daemons
# Core Asynchronous Multiprocessing Module
# ==============================================================================

import asyncio
import logging
import multiprocessing
import sys
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# Global context for system multiprocessing
_context = multiprocessing.get_context()

def set_context(method: str = None) -> None:
    """
    Set the multiprocessing context.
    Allows dynamic switching between 'spawn', 'fork', or 'forkserver'
    depending on the containerized deployment environment.
    """
    global _context
    _context = multiprocessing.get_context(method)

def set_start_method(method: str) -> None:
    """Set the start method for multiprocessing at the application level."""
    multiprocessing.set_start_method(method)

class Process:
    """
    An asyncio-compatible wrapper for multiprocessing.Process.
    Optimized for managing MontiAI system daemons and progressive web applications.
    """
    def __init__(
        self,
        group: Any = None,
        target: Callable[..., Coroutine[Any, Any, Any]] = None,
        name: str = None,
        args: Tuple[Any, ...] = (),
        kwargs: Dict[str, Any] = None,
        *,
        daemon: bool = None,
    ) -> None:
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}
        
        # Initialize the underlying multiprocessing.Process
        self.process = _context.Process(
            group=group,
            target=self.run,
            name=name,
            daemon=daemon,
        )

    def run(self) -> None:
        """
        Entry point for the child process.
        Bootstraps a new asyncio event loop isolated to this process.
        """
        if self.target is None:
            return

        try:
            # Execute the coroutine within a freshly instantiated event loop
            asyncio.run(self.target(*self.args, **self.kwargs))
        except Exception as e:
            log.exception(f"Exception in async process {self.process.name}: {e}")
            sys.exit(1)

    def start(self) -> None:
        """Start the child process."""
        self.process.start()

    async def join(self, timeout: Optional[float] = None) -> None:
        """
        Asynchronously wait for the process to finish.
        Yields control back to the primary event loop while polling the process state.
        """
        # A non-blocking wait utilizing asyncio to prevent event loop thread locking
        while self.process.is_alive():
            await asyncio.sleep(0.005)
            
        if self.process.exitcode != 0 and self.process.exitcode is not None:
            log.warning(f"Process {self.name} exited with code {self.exitcode}")

    @property
    def name(self) -> str:
        return self.process.name

    @property
    def pid(self) -> Optional[int]:
        return self.process.pid

    @property
    def exitcode(self) -> Optional[int]:
        return self.process.exitcode

class Worker(Process):
    """
    A specialized worker process designed to execute network tasks or process
    decentralized node computations from a queue or scheduler. Ideal for 
    handling asynchronous operations within the MontiDroid architecture.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Future network configurations, DNS overrides, or token-based 
        # infrastructure parameters can be initialized here.
