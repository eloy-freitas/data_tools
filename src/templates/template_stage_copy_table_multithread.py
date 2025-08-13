import time
from src.utils.log.log_utils import LogUtils
from src.utils.table.table_manager import TableManager
from sqlalchemy.engine import Engine as _Engine
from src.monitors.monitor import Monitor
from src.workers.sqlalchemy_producer import SQLAlchemyProducer
from src.workers.sqlalchemy_consumer import SQLAlchemyConsumer


class StageCopyTableMultiThread:
    def __init__(
        self,
        table_name_source: str,
        table_name_target: str,
        conn_input: _Engine,
        conn_output: _Engine,
        log_utils: LogUtils,
        table_manager: TableManager,
        consumers: int = 2,
        monitor_timeout:int = 5,
        monitor_buffer_size: int = 10,
        max_rows_buffer:int = 100000,
        chunksize: int = 20000
    ) -> None:
        self._table_name_source = table_name_source
        self._table_name_target = table_name_target
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._consumers = consumers
        self._monitor_timeout = monitor_timeout
        self._monitor_buffer_size = monitor_buffer_size
        self._max_rows_buffer = max_rows_buffer
        self._chunksize = chunksize
        self._log_utils = log_utils
        self._table_manager = table_manager
        self._logger = self._log_utils.get_logger(__name__)
            
    def init_services(self):
        columns = self._table_manager.get_table_columns(conn=self._conn_output, table_name=self._table_name_source)
        query = self._table_manager.create_select_query(table_name=self._table_name_source, columns=columns)

        self._monitor = Monitor(self._monitor_buffer_size, self._monitor_timeout)
        
        self._monitor.subscribe(
            SQLAlchemyProducer(
                monitor=self._monitor,
                engine=self._conn_input,
                query=query,
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
        try:
            start = time.time()
            self._logger.info(f'table source: {self._table_name_source}')
            self._logger.info(f'table target: {self._table_name_target}')
            self._logger.info(f'connection input: {self._conn_input.__repr__()}')
            self._logger.info(f'connection output: {self._conn_output.__repr__()}')

            self._logger.info('starting services...\n')
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