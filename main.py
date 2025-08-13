from src.utils.log.log_utils import LogUtils
from src.utils.table.table_manager import TableManager
from src.templates.template_stage_copy_table_multithread import StageCopyTableMultiThread
from src.connection.postgres_connection_factory import PostgresConnectionFactory


def main():
    """
    Executes a multi-threaded copy of data from one PostgreSQL table to another.
    
    The function sets up database connections, logging, and table management utilities, then initiates a multi-threaded copy operation from the source table to the target table using specified chunk size and concurrency parameters.
    """
    table_name_source = "tabela_2"

    table_name_target = 'tabela_2_target'

    postgres_conn_factory = PostgresConnectionFactory()

    log_utils = LogUtils()

    table_manager = TableManager()

    conn_id = 'dbdw'

    path_conn_file = 'src/resources/postgres_connections.json'

    db_conn = postgres_conn_factory.create_engine_by_file(
        conn_id=conn_id, 
        file_path=path_conn_file
    )

    stage = StageCopyTableMultiThread(
        table_name_source=table_name_source,
        table_name_target=table_name_target,
        conn_input=db_conn,
        conn_output=db_conn,
        chunksize=40000,
        consumers=5,
        monitor_buffer_size=20,
        log_utils=log_utils,
        table_manager=table_manager
    )

    stage.run()

if __name__ == '__main__':
    main()
