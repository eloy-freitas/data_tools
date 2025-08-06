import time
from src.utils.log.log_utils import LogUtils
from src.utils.table.table_manager import TableManager
from sqlalchemy.engine import Engine as _Engine
from src.utils.table.table_manager import TableManager
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
        """
        Initialize a StageCopyTableMultiThread instance for multithreaded table copying.
        
        Parameters:
            table_name_source (str): Name of the source table to copy data from.
            table_name_target (str): Name of the target table to copy data into.
            consumers (int, optional): Number of consumer threads to use for writing data. Defaults to 2.
            monitor_timeout (int, optional): Timeout in seconds for the monitor's buffer operations. Defaults to 5.
            monitor_buffer_size (int, optional): Maximum number of data chunks held in the monitor's buffer. Defaults to 10.
            max_rows_buffer (int, optional): Maximum number of rows buffered before writing. Defaults to 100000.
            chunksize (int, optional): Number of rows to fetch per chunk from the source table. Defaults to 20000.
        
        Sets up internal state for managing the producer-consumer workflow and logging.
        """
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
        """
        Initializes the producer-consumer services and monitor for multithreaded table copying.
        
        Retrieves the source table columns and constructs a select query. Sets up a monitor with the configured buffer size and timeout, subscribes a producer to read data from the source table, and subscribes multiple consumers to write data to the target table.
        """
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
        """
        Executes the multithreaded ETL process to copy data from the source table to the target table.
        
        This method initializes producer-consumer services, truncates the target table, starts the data transfer process, and waits for completion. Logs key steps and the total execution time.
        """
        start = time.time()
        self._logger.info(f'table source: {self._table_name_source}')
        self._logger.info(f'table target: {self._table_name_target}')
        self._logger.info(f'connection input: {self._conn_input.__repr__()}')
        self._logger.info(f'connection output: {self._conn_output.__repr__()}')

        self._logger.info('stating services...\n')
        self.init_services()
        self._logger.info(f'truncating table: {self._table_name_target}...\n')
        self._table_manager.truncate_table(self._conn_output, self._table_name_target)
        self._logger.info('processing etl...\n')
        self._monitor.start()
        self._monitor._end_process.wait()
        end = time.time() - start
        self._logger.info(f'execution time: {end}')
