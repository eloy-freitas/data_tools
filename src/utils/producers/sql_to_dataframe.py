import pandas as _pd
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Engine as _Engine
from .base_producer import BaseProducer


class SQLToDataframe(BaseProducer):
    def __init__(
        self, 
        conn_input:_Engine, 
        query:str,
        chunksize:int=10000
    ) -> None:
        """
        Essa classe tem o objetivo de executar uma consulta no banco de dados e retornar um `pandas.DataFrame`.

        Args:
            conn_input (_Engine): Engine de conexão com banco de dados.
            query (str): Query que vai ser executada no banco de dados
            chunksize (int, optional): Tamanho dos lote de dados.
        """
        if not isinstance(conn_input, _Engine):
            raise TypeError('Objeto de conexão inválido')
        if not isinstance(query, str):
            raise TypeError('Query deve ser uma string')
        
        self._conn_input = conn_input
        self._query = query
        self._chunksize = chunksize

    def extract(self):
        """
        Executa a consulta no bando de dados e retorna um `pandas.DataFrame`.

        Returns:
            `pandas.Dataframe`: Resultado da consulta.
        """
        try:  
            return _pd.read_sql_query(
                sql=self._query, 
                con=self._conn_input,
                chunksize=self._chunksize
            )
        except _SQLAlchemyError as e:
            raise _SQLAlchemyError(f"Falha ao extrair dados: {e}")