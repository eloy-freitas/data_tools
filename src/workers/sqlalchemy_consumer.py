from sqlalchemy.engine import Engine as _Engine
from .base_worker import BaseWorker as _BaseWorker
from src.monitors.base_monitor import BaseMonitor as _BaseMonitor
from src.utils.table.table_manager import TableManager as _TableManager


class SQLAlchemyConsumer(_BaseWorker):
    def __init__(
        self,
        monitor: _BaseMonitor,
        engine: _Engine,
        table_manager: _TableManager,
        columns: list[str],
        table_name_target: str,
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
        super().__init__(
            monitor=monitor,
            is_producer=False,
        )
        
        self._engine = engine
        self._table_manager = table_manager
        self._insert_query_template = self._table_manager.build_insert_query(table_name=table_name_target, columns=columns)

    def run(self):
        """
        Método executado pela thread responsável por escrever os dados extraídos do monitor no banco de dados.
        """
        with self._engine.connect() as conn:
            """
            Enquando a flag `_stop` for False o thread consome dados do monitor e insere no banco de dados.
            """
            while not self._stop.is_set():
                data = self._monitor.read()
                if data:
                    try:
                        self._table_manager.insert(
                            conn=conn,
                            data=data,
                            insert_query_template=self._insert_query_template
                        )
                    except Exception as e:
                        self.stop_all_workers()
                        raise Exception(e)
        