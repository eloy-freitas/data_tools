from sqlalchemy.engine import Engine as _Engine
from src.utils.table.table_manager import TableManager
from src.monitors.monitor import Monitor
from src.workers.sqlalchemy_producer import SQLAlchemyProducer
from src.workers.sqlalchemy_consumer import SQLAlchemyConsumer
import time


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
        yield_per: int = 20000
    ) -> None:
        self._query = query
        self._table_name_taget = table_name_taget
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._consumers = consumers
        self._monitor_timeout = monitor_timeout
        self._monitor_buffer_size = monitor_buffer_size
        self._max_rows_buffer = max_rows_buffer
        self._yield_per = yield_per
            
    def init_services(self):
        self._table_manager = TableManager()

        self._monitor = Monitor(self._monitor_buffer_size, self._monitor_timeout)

        self._monitor.subscribe(
            SQLAlchemyProducer(
                monitor=self._monitor,
                engine=self._conn_input,
                query=self._query,
                max_rows_buffer=self._max_rows_buffer,
                yield_per=self._yield_per,
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
        print('iniciando os serviços\n')
        self.init_services()
        print('truncando tabela de destino\n')
        self._table_manager.truncate_table(self._conn_output, self._table_name_taget)
        print('executando os threads\n')
        self._monitor.start()
        print('esperando threads finalizar a execução\n')
        self._monitor._end_process.wait()
        end = time.time() - start
        print(end)
