from sqlalchemy.engine import Engine as _Engine
from .base_worker import BaseWorker as _BaseWorker
from src.monitors.monitor import Monitor as _Monitor
from src.utils.table.table_manager import TableManager as _TableManager


class SQLAlchemyConsumer(_BaseWorker):
    def __init__(
        self,
        monitor: _Monitor,
        engine: _Engine,
        table_manager: _TableManager,
        columns: list[str],
        table_name_target: str,
    ) -> None:
        super().__init__(
            monitor=monitor,
            is_producer=False,
        )
        
        self._engine = engine
        self._table_manager = table_manager
        self._insert_query_template = self._table_manager.build_insert_query(table_name=table_name_target, columns=columns)

    def run(self):
        conn = self._engine.raw_connection()
        
        cursor = conn.cursor()

        while not self._stop.is_set():
            data = self._monitor.read()
            if data:
                try:
                    self._table_manager.insert(
                        data=data,
                        insert_query_template=self._insert_query_template,
                        conn=conn,
                        cursor=cursor
                    )
                except Exception as e:
                    self.stop_all_workers()
                    cursor.close()
                    conn.close()
                    raise Exception(e)
    
        cursor.close()
        conn.close()