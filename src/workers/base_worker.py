from typing import Any
from threading import (
    Thread as _Thread, 
    Event as _Event 
)
from abc import abstractmethod as _abstractmethod
from src.monitors.monitor import Monitor as _Monitor

class BaseWorker(_Thread):
    def __init__(
        self
        , monitor: _Monitor 
        , is_producer: bool = False
    ) -> None:
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
    def run(self): ...

    def stop(self):
        self._stop.set()

    def stop_all_workers(self):
        self._monitor.stop_all_workers()