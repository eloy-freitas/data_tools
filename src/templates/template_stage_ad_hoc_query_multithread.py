import time
from sqlalchemy.engine import Engine as _Engine

from src.utils.table.table_manager import TableManager
from src.monitors.monitor import Monitor
from src.utils.log.log_utils import LogUtils
from src.workers.sqlalchemy_producer import SQLAlchemyProducer
from src.workers.sqlalchemy_consumer import SQLAlchemyConsumer


class StageAdHocMultiThread:
    def __init__(
        self,
        query: str,
        table_name_target: str,
        conn_input: _Engine,
        conn_output: _Engine,
        table_manager: TableManager,
        log_utils: LogUtils,
        consumers: int = 2,
        monitor_timeout:int = 5,
        monitor_buffer_size: int = 10,
        max_rows_buffer:int = 100000,
        chunksize: int = 20000
    ) -> None:
        self._query = query
        self._table_name_target = table_name_target
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._consumers = consumers
        self._monitor_timeout = monitor_timeout
        self._monitor_buffer_size = monitor_buffer_size
        self._max_rows_buffer = max_rows_buffer
        self._chunksize = chunksize
        self._table_manager = table_manager
        self._log_utils = log_utils
        self._logger = self._log_utils.get_logger(__name__)
            
    def init_services(self):
        self._monitor = Monitor(self._monitor_buffer_size, self._monitor_timeout)

        self._monitor.subscribe(
            SQLAlchemyProducer(
                monitor=self._monitor,
                engine=self._conn_input,
                query=self._query,
                max_rows_buffer=self._max_rows_buffer,
                chunksize=self._chunksize,
                table_manager=self._table_manager,
                table_target=self._table_name_target
            )
        )

        for _ in range(self._consumers):
            self._monitor.subscribe(
                SQLAlchemyConsumer(
                    monitor=self._monitor,
                    engine=self._conn_output,
                    table_manager=self._table_manager
                )
            )
            
    def run(self):
        start = time.time()
        try:
            self._logger.info(f'query input: \n{self._query}')
            self._logger.info(f'table target: {self._table_name_target}')
            self._logger.info(f'connection input: {self._conn_input.__repr__()}')
            self._logger.info(f'connection output: {self._conn_output.__repr__()}')

            self._logger.info('stating services...\n')
            self.init_services()
            self._logger.info(f'truncating table: {self._table_name_target}...\n')
            self._table_manager.truncate_table(self._conn_output, self._table_name_target)
            self._logger.info('processing etl...\n')
            self._monitor.start()
            self._monitor.wait_for_completion()
            end = time.time() - start
            self._logger.info(f'execution time: {end}')
        except Exception as e: 
            self._logger.error(f'ETL process failed: {e}') 
            raise
