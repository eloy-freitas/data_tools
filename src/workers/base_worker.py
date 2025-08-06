from typing import Any
from threading import (
    Thread as _Thread, 
    Event as _Event 
)
from abc import abstractmethod as _abstractmethod
from src.monitors.monitor import Monitor as _Monitor

class BaseWorker(_Thread):
    def __init__(
        self,
        monitor: _Monitor,
        is_producer: bool = False
    ) -> None:
        """
        Initialize a BaseWorker thread with a monitor and an optional producer flag.
        
        Parameters:
            monitor (_Monitor): The monitor instance used to coordinate or manage worker threads.
            is_producer (bool, optional): Indicates if this worker acts as a producer. Defaults to False.
        """
        super().__init__(
            group=None, 
            target=None, 
            name=None, 
            args=None,
            kwargs=None, 
            daemon=False
        )
        self._stop = _Event()
        self._monitor = monitor
        self._is_producer: bool = is_producer
    
    @_abstractmethod
    def run(self): """
Defines the main execution logic for the worker thread.

This method must be implemented by subclasses to specify the thread's behavior.
"""
...

    def stop(self):
        """
        Signal the worker thread to stop by setting the internal stop event.
        """
        self._stop.set()

    def stop_all_workers(self):
        """
        Instruct the associated monitor to stop all worker threads it manages.
        """
        self._monitor.stop_all_workers()