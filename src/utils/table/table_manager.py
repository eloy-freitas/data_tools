from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
import pandas as pd


class TableManager:
    def __init__(self) -> None:
        pass

    def truncate_table(self, conn:_Engine, table_name:str, schema: str = None):
        with conn.connect() as conn:
            try:
                conn.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao truncar tabela: {e}")
            
    def get_max(self, conn:_Engine, table_name:str, column:str, schema: str = None):
        max = 0

        with conn.connect() as conn:
            try:
                result = conn.execute(f"SELECT COALESCE(MAX({column}),0) FROM {schema}.{table_name}")
                max = result.fetchone()[0]
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao truncar tabela: {e}")
            
        return max
    
    def execute_update(self, conn:_Engine, query, payload=None):
        with conn.connect() as conn:
            with conn.begin() as session:
                try:
                    conn.execute(query, payload)
                except _SQLAlchemyError as e:
                    raise _SQLAlchemyError(f"Falha ao executar query: {e}") 

    def count(self, conn:_Engine, table_name:str, schema: str = None):
        with conn.connect() as con:
            try:
                cursor = con.execute(f"SELECT COUNT(1) FROM {schema}.{table_name}")
                result = cursor.fetchone()[0]
                return result
                
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao executar contagem: {e}")
    
    def execute_query(self, conn:_Engine, query):
        with conn.connect() as conn:
            with conn.begin() as session:
                try:
                    conn.execute(query)
                except _SQLAlchemyError as e:
                    raise _SQLAlchemyError(f"Falha ao executar query: {e}")
    
    def get_table_columns(self, conn:_Engine, table_name: str):
        with conn.connect() as con:
            try:
                cursor = con.execute(f"SELECT * FROM {table_name} limit 1")
                
                result = cursor.keys()

                columns = list(result)
                
                return columns
                
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Falha ao executar contagem: {e}")
                    