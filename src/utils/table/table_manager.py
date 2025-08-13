from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Connection as _Connection
from sqlalchemy import text


class TableManager:
    def __init__(self) -> None:
        """
        Initialize a new instance of the TableManager class.
        """
        pass

    def truncate_table(self, conn:_Engine, table_name:str, schema: str = None) -> None:
        """
        Truncates all rows from the specified table in the database.
        
        If a schema is provided, the table name is qualified with the schema. Raises a SQLAlchemyError with a custom message if the operation fails.
        """
        if schema:
            table_name = f"{schema}.{table_name}"
        with conn.connect() as conn:
            with conn.begin():
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                except _SQLAlchemyError as e:
                    raise _SQLAlchemyError(f"Fail to truncate table: {e}")
    
    def get_table_columns(self, conn:_Engine, table_name: str, schema:str = None) -> list[str]:
        """
        Retrieve the column names of a specified table.
        
        Parameters:
        	table_name (str): Name of the table to inspect.
        	schema (str, optional): Schema name to qualify the table, if applicable.
        
        Returns:
        	list[str]: List of column names in the table.
        
        Raises:
        	SQLAlchemyError: If the query to retrieve columns fails.
        """
        if schema:
            table_name = f"{schema}.{table_name}"

        with conn.connect() as con:
            try:
                cursor = con.execute(text(f"SELECT * FROM {table_name} limit 1"))
                
                result = cursor.keys()

                columns = list(result)
                
                return columns
                
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(f"Fail to get columns from table {table_name}: {e}")
    
    def create_select_query(
        self, 
        table_name: str, 
        columns: list[str], 
        schema: str = None, 
        ignore_columns: list[str] = None
    ) -> str:
        """
        Construct a SQL SELECT query string for the specified table and columns, optionally excluding specified columns.
        
        Parameters:
            table_name (str): Name of the table to query.
            columns (list[str]): List of column names to include in the SELECT statement.
            schema (str, optional): Schema name to prefix the table with.
            ignore_columns (list[str], optional): List of column names to exclude from the SELECT statement.
        
        Returns:
            str: The constructed SQL SELECT query string.
        
        Raises:
            ValueError: If column removal fails due to invalid input.
        """
        if schema:
            table_name = f"{schema}.{table_name}"

        try:
            if ignore_columns:
                for c in ignore_columns:
                    columns.remove(c)
            columns_str = ','.join(columns)
        except ValueError as e:
            raise ValueError(f'Invalid type of columns. Use a list of strings. \n{e}')

        return f"SELECT {columns_str} FROM {table_name}"

                    
    def insert(
        self,
        data: object,
        conn: _Connection,
        cursor: object,
        insert_query_template: str
    ) -> None:
        """
        Performs a batch insert of data into a database table using the provided insert query template.
        
        If the insert operation fails, the transaction is rolled back and a SQLAlchemyError is raised with an error message.
        """
        try:
            cursor.executemany(insert_query_template, data)
            conn.commit()
        except _SQLAlchemyError as e:
            conn.rollback()
            raise _SQLAlchemyError(
                f"Fail to insert data \n {e}"
            )
    
    def build_insert_query(self, table_name: str,columns: list[str]) -> str:
        """
        Generate an SQL INSERT INTO query template for the specified table and columns.
        
        Parameters:
            table_name (str): Name of the table to insert into.
            columns (list[str]): List of column names for the insert operation.
        
        Returns:
            str: An SQL INSERT statement template with parameter placeholders for each column.
        """
        columns_names_str = ",".join(columns)
        columns_name_parametes = ",".join([f"%s" for _ in columns])

        insert_query_template = f"""
            INSERT INTO {table_name}({columns_names_str}) VALUES ({columns_name_parametes})
        """

        return insert_query_template