import pandas as _pd
from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError

class DataframeToSQL:
    """
    Essa classe te o objetivo de fazer a carga dos dados de um objeto `pandas.DataFrame` no banco de dados.
    """
    def __init__(
        self, 
        dataframe: _pd.DataFrame, 
        conn_output:_Engine, 
        table_name:str,
        chunksize:int,
        schema:str,
    ):
        """_summary_

        Args:
            dataframe (_pd.DataFrame): Objeto da dados para ser carregado no banco de dados.
            conn_output (_Engine): Engine de conexão com banco de dados.
            table_name (str): Nome da tabela alvo.
            chunksize (int): Tamanho dos lotes que vão ser transferidos.
            schema (str): Schema do banco de dados.
        """
        if not isinstance(dataframe, _pd.DataFrame):
            raise ValueError("Dataset não é um dataframe")

        if not isinstance(conn_output, _Engine):
            raise ValueError("Objeto de conexão inválido")
    
        if not isinstance(table_name, str):
            raise ValueError("Nome da tabela deve ser uma string")
        
        if not isinstance(schema, str):
            raise ValueError("Schema deve ser uma string")

        if not isinstance(chunksize, int):
            raise ValueError("Cunksize deve ser um inteiro")

        self._dataframe = dataframe
        self._conn_output = conn_output
        self._table_name = table_name
        self._chunksize = chunksize
        self._schema = schema
        

    def load(self):
        """
        Método responsável por fazer a carga dos dados.

        Raises:
            _SQLAlchemyError: _description_
        """
        try:
            """
            Cria conexão com o banco de dados.
            """
            with self._conn_output.connect() as conn:
                """
                Abre uma transação com o banco de dados.
                """
                with conn.begin():
                    self._dataframe.to_sql(
                        name=self._table_name,
                        con=conn,
                        index=False,
                        if_exists='append',
                        chunksize=self._chunksize,
                        schema=self._schema
                    )
        except _SQLAlchemyError as e:
            raise _SQLAlchemyError(f"Falha ao carregar dados: {e}")