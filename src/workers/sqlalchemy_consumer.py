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
    ) -> None:
        """
        Initialize a SQLAlchemyConsumer with a monitor, database engine, and table manager.
        
        Parameters:
            monitor (_Monitor): The monitor instance used for data coordination.
            engine (_Engine): The SQLAlchemy engine for database connections.
            table_manager (_TableManager): The manager responsible for database table operations.
        """
        super().__init__(
            monitor=monitor,
            is_producer=False,
        )
        
        self._engine = engine
        self._table_manager = table_manager

    def run(self):
        """
        Continuously reads data from the monitor and inserts it into the database until a stop event is triggered.
        
        If an error occurs during insertion, all workers are stopped and the exception is re-raised. After completion or error, database resources are closed and the monitor is notified that processing has ended.
        """
        self._insert_query_template = self._monitor.get_insert_query()
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
        self._monitor._end_process.set()