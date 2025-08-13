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
        """
        Initialize a SQLAlchemyProducer to stream query results from a database and send them to a monitor.
        
        Parameters:
            query (str): The SQL query to execute for data extraction.
            max_rows_buffer (int): Maximum number of rows to buffer during streaming.
            chunksize (int): Number of rows to fetch per chunk from the database.
            table_manager (TableManager): Utility for building insert queries for the target table.
            table_target (str): Name of the target table for data insertion.
        """
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
        """
        Executes a SQL query using SQLAlchemy, streams the results in chunks, and writes each chunk to the monitor.
        
        Establishes a streaming database connection, executes the provided query, and retrieves results in batches of the specified chunk size. For each batch, writes the data to the monitor and checks for stop signals to halt processing if necessary. Handles SQL execution errors and ensures all workers are stopped on failure. Signals completion to the monitor after all data is processed.
        """
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
