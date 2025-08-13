from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from .base_worker import BaseWorker as _BaseWorker
from src.monitors.monitor import Monitor as _Monitor
from sqlalchemy import text
from src.utils.table.table_manager import TableManager

class SQLAlchemyProducer(_BaseWorker):
    def __init__(
        self,
        monitor: _Monitor, 
        engine: _Engine, 
        query: str, 
        max_rows_buffer: int, 
        chunksize: int,
        table_manager: TableManager,
        table_target: str
    ) -> None:
        super().__init__(
            monitor=monitor,
            is_producer=True,
        )
        self._engine=engine
        self._query=query
        self._max_rows_buffer=max_rows_buffer
        self._chunksize=chunksize
        self._table_manager = table_manager
        self._table_target = table_target
        

    def run(self):
        with self._engine.connect().execution_options(
            stream_results=True, max_rows_buffer=self._max_rows_buffer
        ) as conn:
            try:
                cursor = conn.execute(text(self._query)).yield_per(self._chunksize) 
            except _SQLAlchemyError as e:
                self.stop_all_workers()
                conn.close()
                raise _SQLAlchemyError(
                    f"Fail to insert data \n {e}"
                )
            columns = cursor.keys()

            insert_query = self._table_manager.build_insert_query(
                table_name=self._table_target,
                columns=columns
            )

            self._monitor.set_insert_query(insert_query)

            while result := cursor.fetchmany():
                try:
                    if self._stop.is_set():
                        raise RuntimeError("Stopping data production due to error in another worker.")
                    else:
                        self._monitor.write(result)
                except Exception as e:
                    self.stop_all_workers()
                    raise Exception("Failed during data fetching")
        
        self._monitor.producer_end_process()
