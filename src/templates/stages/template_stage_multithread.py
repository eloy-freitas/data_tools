from sqlalchemy.engine import Engine as _Engine
from src.utils.table.table_manager import TableManager
from src.monitors.monitor import Monitor
from src.workers.sqlalchemy_producer import SQLAlchemyProducer
from src.workers.sqlalchemy_consumer import SQLAlchemyConsumer
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

logger = logging.getLogger(__name__)



class StageMultiThread:
    def __init__(
        self,
        query: str,
        table_name_taget: str,
        conn_input: _Engine,
        conn_output: _Engine,
        consumers: int = 2,
        monitor_timeout:int = 5,
        monitor_buffer_size: int = 10,
        max_rows_buffer:int = 100000,
        chunksize: int = 20000
    ) -> None:
        self._query = query
        self._table_name_taget = table_name_taget
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._consumers = consumers
        self._monitor_timeout = monitor_timeout
        self._monitor_buffer_size = monitor_buffer_size
        self._max_rows_buffer = max_rows_buffer
        self._chunksize = chunksize
            
    def init_services(self):
        self._table_manager = TableManager()

        self._monitor = Monitor(self._monitor_buffer_size, self._monitor_timeout)

        self._monitor.subscribe(
            SQLAlchemyProducer(
                monitor=self._monitor,
                engine=self._conn_input,
                query=self._query,
                max_rows_buffer=self._max_rows_buffer,
                chunksize=self._chunksize,
                table_manager=self._table_manager,
                table_target=self._table_name_taget
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
            
    def start(self):
        start = time.time()
        logger.info('stating services...\n')
        self.init_services()
        logger.info(f'truncating table {self._table_name_taget}...\n')
        self._table_manager.truncate_table(self._conn_output, self._table_name_taget)
        logger.info('starting workers...\n')
        self._monitor.start()
        logger.info('waiting workers...\n')
        self._monitor._end_process.wait()
        end = time.time() - start
        logger.info(end)
