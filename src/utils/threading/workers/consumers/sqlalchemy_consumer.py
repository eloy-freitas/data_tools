from typing import Any
from sqlalchemy.engine import Engine as _Engine
from ..base_worker import BaseWorker as _BaseWorker
from collections.abc import Callable, Iterable, Mapping
from ...monitors.base_monitor import BaseMonitor as _BaseMonitor 
from ....consumers.sqlalchemy_batch_consumer import SQLAlchemyBatchConsumer as _SQLAlchemyBatchConsumer


class SQLAlchemyConsumer(_BaseWorker):
    def __init__(
        self,
        monitor: _BaseMonitor,
        engine: _Engine,
        columns: list[str],
        table_name_target: str,
        group=None,
        target: Callable[..., object] = None,
        name: str = None,
        args: Iterable[Any] = None,
        kwargs: Mapping[str, Any] = None,
        daemon: bool = None
    ) -> None:
        """
        Especialização da classe Thread responsável por consumir dados de um `Monitor`
        e escrever no banco de dados.

        Args:
            monitor (_BaseMonitor): Referência do objeto monitor no qual foi inscrito.
            engine (_Engine): Engine de conexão com banco de dados.
            columns (list[str]): Colunas da tabela de destino dos dados.
            table_name_target (str): Nome da tabela de destino dos dados.
        """
        is_producer: bool = False
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
        
        self._engine = engine
        """
        Classe especializada para escrever no banco de dados.
        """
        self._consumer = _SQLAlchemyBatchConsumer(
            columns=columns,
            table_name_target=table_name_target
        )
        
    def run(self):
        """
        Método executado pela thread responsável por escrever os dados extraídos do monitor no banco de dados.
        """
        with self._engine.connect() as conn:
            """
            Enquando a flag `_stop` for False o thread consome dados do monitor e insere no banco de dados.
            """
            while not self._stop.isSet():
                data = self._monitor.read()
                if data:
                    try:
                        self._consumer.load(
                            data=data,
                            conn=conn
                        )
                    except Exception as e:
                        self.stop_all_workers()
                        raise Exception(e)

            