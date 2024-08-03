from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Connection as _Connection
from typing import Any


class SQLAlchemyBatchConsumer():
    def __init__(
        self,
        columns: list[str],
        table_name_target: str
    ) -> None:
        """
        Essa classe tem o objetivo de fazer insert no banco de dados por meio do `SQLAlchemy`.

        Args:
            columns (list[str]): Lista de colunas que vão ser inseridas.
            table_name_target (str): Nome da tabela alvo.
        """
        self._columns: list[str] = columns
        self._table_name: str = table_name_target
        self.build_insert_query()

    def build_insert_query(self):
        """
        Esse método tem o objetivo de construir a instrução de insert com base nos parâmetros da classe.
        """
        columns_names_str = ",".join(self._columns)
        columns_name_parametes = ",".join([f"%s" for _ in self._columns])

        self._insert_query_template = f"""
            INSERT INTO {self._table_name}({columns_names_str}) VALUES ({columns_name_parametes})
        """

    def load(self, data: object, conn: _Connection):
        """
        Realiza a inserção no banco de dados.

        Args:
            data (object): Objeto de dados.
            conn (_Connection): Objeto de conexão do banco de dados.
            
        """
        with conn.begin() as transaction:
            try:
                conn.execute(self._insert_query_template, data)
                transaction.commit()
            except _SQLAlchemyError as e:
                transaction.rollback()
                raise _SQLAlchemyError(
                    f"Falha ao inserir dados \n"
                    f"MENSAGEM DE ERRO: {e}"
                )
            