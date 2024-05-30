from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Connection as _Connection
from typing import Any


class SQLAlchemyBatchConsumer():
    def __init__(
        self,
        columns: list[str],
        table_name_target: str
    ) -> None:
        self._columns: list[str] = columns
        self._table_name: str = table_name_target
        self.build_insert_query()

    def build_insert_query(self):
        columns_names_str = ",".join(self._columns)
        columns_name_parametes = ",".join([f"%s" for _ in self._columns])

        self._insert_query_template = f"""
            INSERT INTO {self._table_name}({columns_names_str}) VALUES ({columns_name_parametes})
        """

    def load(self, data: object, conn: _Connection):
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
            