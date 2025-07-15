from sqlalchemy.engine import Engine as _Engine
from src.utils.table.table_manager import TableManager
from src.monitors.monitor import Monitor
from src.workers.sqlalchemy_producer import SQLAlchemyProducer
from src.workers.sqlalchemy_consumer import SQLAlchemyConsumer


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
            """
            Classe template para fazer extração de uma tabela muito grande no banco de dados e armazenar em uma tabela stage
            utilzando multiplos threads.


            Args:
                query (str): Query que vai ser executada no banco de dados.
                table_name_taget (str): Nome da tabela que vai ser carregada.
                conn_input (_Engine): Engine da conexão de origem de dados.
                conn_output (_Engine): Engine da conexão de destino de dados.
                consumers (int, optional): Quantidade de threads que vão escrever no banco de dados. Por padrão 2.
                monitor_timeout (int, optional): Quantidade de tempo que um thread vai ficar em espera no monitor. Por padrão 5 segundos.
                monitor_buffer_size (int, optional): Tamanho do buffer do monitor. Por padrão 10 slots.
                max_rows_buffer (int, optional): Tamanho do buffer do DBAPI. Por padrão 100000.
                yield_per (int, optional): Quantidades de dados que vão ser retornadas em cada fetch. Por padrão 20000.
            """
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
        """
        Esse método prepara os threads que vão ser executados.
        """

        self._table_manager = TableManager()

        columns = self._table_manager.get_table_columns(
            self._conn_output, 
            self._table_name_taget
        )

        self._monitor = Monitor(self._monitor_buffer_size, self._monitor_timeout)

        self._monitor.subscribe(
            SQLAlchemyProducer(
                monitor=self._monitor,
                engine=self._conn_input,
                query=self._query,
                max_rows_buffer=self._max_rows_buffer,
                yield_per=self._yield_per,
                is_producer=True
            )
        )

        for _ in range(self._consumers):
            self._monitor.subscribe(
                SQLAlchemyConsumer(
                monitor=self._monitor,
                engine=self._conn_output,
                columns=columns,
                table_name_target=self._table_name_taget
            )
        )
            
    def start(self):
        """
        Esse método inicia os serviços da classe e inicia a execução dos threads.
        """
        print('iniciando os serviços\n')
        self.init_services()
        print('executando os threads\n')
        self._monitor.start()
        print('truncando tabela de destino\n')
        self._table_manager.truncate_table(self._conn_output, self._table_name_taget)
        print('esperando threads finalizar a excução\n')
        self._monitor._end_process.wait()
