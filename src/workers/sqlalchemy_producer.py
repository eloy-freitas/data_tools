from typing import Any
from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from .base_worker import BaseWorker as _BaseWorker
from collections.abc import Callable, Iterable, Mapping
from src.monitors.base_monitor import BaseMonitor as _BaseMonitor



class SQLAlchemyProducer(_BaseWorker):
    def __init__(
        self,
        monitor: _BaseMonitor, 
        engine: _Engine, 
        query: str, 
        max_rows_buffer: int, 
        yield_per: int, 
        is_producer: bool = True, 
        group=None, 
        target: Callable[..., object] = None, 
        name: str = None, 
        args: Iterable[Any] = None, 
        kwargs: Mapping[str, Any] = None, 
        daemon: bool = None
    ) -> None:
        """
        Especialização da classe Thread responsável por fazer leitura no banco de dados 
        e escrever no `Monitor`.

        Args:
            monitor (_BaseMonitor): Referência do objeto monitor no qual foi inscrito.
            engine (_Engine): Engine de conexão com banco de dados.
            query (str): Query select para extração dos dados.
            max_rows_buffer (int): Quantidade de linhas do buffer do DBAPI.
            yield_per (int): Quantidade de linhas que vão se extraídas do buffer do DBAPI.
            is_producer (bool, optional): Flag para identifcar o thread como produtor. Defaults to True.
        """
        super().__init__(
            monitor,
            is_producer,
            group,
            target,
            name,
            args,
            kwargs,
            daemon=daemon
        )
        self._engine=engine
        self._query=query
        self._max_rows_buffer=max_rows_buffer
        self._yield_per=yield_per
        

    def run(self):
        """
        Método responsável que executa a consulta select no banco de dados e faz a extração dos
        dados em lotes para escrever em memória.
        """

        """
        Cria o objeto de conexão com o banco de dados e configura o cursor no lado do servidor para realizar stream de dados.
        Além disso, configura o tamanho máximo do buffer de dados no lado do cliente.
        """
        with self._engine.connect().execution_options(
            stream_results=True, max_rows_buffer=self._max_rows_buffer
        ) as conn:
            try:
                """
                Executa a consulta no banco de dados e define o tamanho de cada lote de dados que será trazido pelo fetch do cursor.
                """
                cursor = conn.execute(self._query).yield_per(self._yield_per) 
            except _SQLAlchemyError as e:
                self.stop_all_workers()
                conn.close()
                raise _SQLAlchemyError(
                    "ERRO: Falha ao extrair dados \n"
                    f"MENSAGEM DE ERRO: {e}"
                )
            """
            Faz o fetch dos dados que estão no buffer.
            """
            while result := cursor.fetchmany():
                try:
                    """
                    Verifica se a flag para interromper a execução do processo está ativa.
                    Caso contrário, escreve na memória compartilhada.
                    """
                    if self._stop.is_set():
                        raise RuntimeError("Hover um erro. Parando todos os threads.")
                    else:
                        self._monitor.write(result)
                except Exception as e:
                    self.stop_all_workers()
                    raise Exception(e)
        
        """
        Notifica para a memória compartilhada que terminou a execução.
        """
        self._monitor.done()
