from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Connection as _Connection
from sqlalchemy import text


class TableManager:
    def __init__(self) -> None:
        pass

    def truncate_table(self, conn:_Engine, table_name:str, schema: str = None):
        if schema:
            table_name = f"{schema}.{table_name}"
        with conn.connect() as conn:
            try:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao truncar tabela: {e}")
            
    def get_max(self, conn:_Engine, table_name:str, column:str, schema: str = None):
        if schema:
            table_name = f"{schema}.{table_name}"
        max = 0

        with conn.connect() as conn:
            try:
                result = conn.execute(text(f"SELECT COALESCE(MAX({column}),0) FROM {table_name}"))
                max = result.fetchone()[0]
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao truncar tabela: {e}")
            
        return max
    
    def execute_update(self, conn:_Engine, query, payload=None):
        with conn.connect() as conn:
            with conn.begin() as session:
                try:
                    conn.execute(text(query), payload)
                except _SQLAlchemyError as e:
                    raise _SQLAlchemyError(f"Falha ao executar query: {e}") 

    def count(self, conn:_Engine, table_name:str, schema: str = None):
        if schema:
            table_name = f"{schema}.{table_name}"
        with conn.connect() as con:
            try:
                cursor = con.execute(text(f"SELECT COUNT(1) FROM {table_name}"))
                result = cursor.fetchone()[0]
                return result
                
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao executar contagem: {e}")
    
    def execute_query(self, conn:_Engine, query):
        with conn.connect() as conn:
            with conn.begin() as session:
                try:
                    conn.execute(text(query))
                except _SQLAlchemyError as e:
                    raise _SQLAlchemyError(f"Falha ao executar query: {e}")
    
    def get_table_columns(self, conn:_Engine, table_name: str):
        with conn.connect() as con:
            try:
                cursor = con.execute(text(f"SELECT * FROM {table_name} limit 1"))
                
                result = cursor.keys()

                columns = list(result)
                
                return columns
                
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao executar contagem: {e}")
                    
    def insert(self, data: object, conn: _Connection, cursor: object, insert_query_template: str):
        """
        Realiza a inserção no banco de dados.

        Args:
            data (object): Objeto de dados.
            conn (_Connection): Objeto de conexão do banco de dados.
            
        """
        try:
            cursor.executemany(insert_query_template, data)
            conn.commit()
        except _SQLAlchemyError as e:
            conn.rollback()
            raise _SQLAlchemyError(
                f"Falha ao inserir dados \n"
                f"MENSAGEM DE ERRO: {e}"
            )
    
    def build_insert_query(self, table_name: str,columns: list[str]):
        """
        Esse método tem o objetivo de construir a instrução de insert com base nos parâmetros da classe.
        """
        columns_names_str = ",".join(columns)
        columns_name_parametes = ",".join([f"%s" for _ in columns])

        insert_query_template = f"""
            INSERT INTO {table_name}({columns_names_str}) VALUES ({columns_name_parametes})
        """

        return insert_query_template