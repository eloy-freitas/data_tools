from typing import Any
from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from ..base_worker import BaseWorker as _BaseWorker
from collections.abc import Callable, Iterable, Mapping
from ...monitors.base_monitor import BaseMonitor as _BaseMonitor
from ....producers.sqlalchemy_batch_producer import SQLAlchemyBatchProducer as _SQLAlchemyBatchProducer


class SQLAlchemyProducer(_BaseWorker):
    def __init__(
        self,
        monitor: _BaseMonitor, 
        engine: _Engine, 
        query: str, 
        max_rows_buffer: int, 
        yield_per: int, 
        is_producer: bool = True, 
        group=None, 
        target: Callable[..., object] = None, 
        name: str = None, 
        args: Iterable[Any] = None, 
        kwargs: Mapping[str, Any] = None, 
        daemon: bool = None
    ) -> None:
        super().__init__(
            monitor,
            is_producer,
            group,
            target,
            name,
            args,
            kwargs,
            daemon=daemon
        )
        self._engine=engine
        self._query=query
        self._max_rows_buffer=max_rows_buffer
        self._yield_per=yield_per
        

    def run(self):
        with self._engine.connect().execution_options(
            stream_results=True, max_rows_buffer=self._max_rows_buffer
        ) as conn:
            try:
                cursor = conn.execute(self._query).yield_per(self._yield_per) 
            except _SQLAlchemyError as e:
                self.stop_all_workers()
                conn.close()
                raise _SQLAlchemyError(
                    "ERRO: Falha ao extrair dados \n"
                    f"MENSAGEM DE ERRO: {e}"
                )
            while result := cursor.fetchmany():
                try:
                    if not self._stop.isSet():
                        self._monitor.write(result)
                    else:
                        self.stop_all_workers()
                except Exception as e:
                    self.stop_all_workers()
                    raise Exception(e)
        
        self._monitor.done()
