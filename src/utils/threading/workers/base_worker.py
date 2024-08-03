from typing import Any
from threading import (
    Thread as _Thread, 
    Event as _Event 
)
from abc import abstractmethod as _abstractmethod
from collections.abc import Callable, Iterable, Mapping
from src.utils.threading.monitors.base_monitor import BaseMonitor as _BaseMonitor

class BaseWorker(_Thread):
    def __init__(
        self
        , monitor: _BaseMonitor 
        , is_producer: bool = False
        , group = None
        , target: Callable[..., object] = None
        , name: str  = None
        , args: Iterable[Any] = None
        , kwargs: Mapping[str, Any] = None
        , *
        , daemon: bool = None
    ) -> None:
        """
        Especialização da classe Thread para trabalhar de forma sincronizada com memória compartilhada.

        Args:
            monitor (_BaseMonitor): Referência do objeto monitor no qual foi inscrito.
            is_producer (bool, optional): Flag para identificar se o thread vai produzir dados. Defaults to False.
        """
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self._stop = _Event()
        self._monitor = monitor
        self._is_producer: bool = is_producer
    
    @_abstractmethod
    def run(self): ...
    """
    Assínatura do método que será executado pela thread.
    """

    def stop(self):
        """
        Define a flag `_stop` para True, sinalizado que o thread deve parar sua execução.
        """
        self._stop.set()

    def stop_all_workers(self):
        """
        Realiza a chamada do método `stop_all_workers` do monitor para parar a execução dos demais threads.
        """
        self._monitor.stop_all_workers()