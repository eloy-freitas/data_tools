from src.utils.log.log_utils import LogUtils
from src.utils.table.table_manager import TableManager
from src.templates.template_stage_ad_hoc_query_multithread import StageAdHocMultiThread
from src.connection.postgres_connection_factory import PostgresConnectionFactory


def main():
    query = "select * from tabela"

    table_name_target = 'tabela_grande'

    postgres_conn_factory = PostgresConnectionFactory()

    conn_id = 'dbdw'

    path_conn_file = 'src/resources/postgres_connections.json'

    db_conn = postgres_conn_factory.create_engine_by_file(
        conn_id=conn_id, 
        file_path=path_conn_file
    )

    table_manager = TableManager()

    log_utils = LogUtils()

    stage = StageAdHocMultiThread(
        query=query,
        table_name_target=table_name_target,
        conn_input=db_conn,
        conn_output=db_conn,
        table_manager=table_manager,
        chunksize=40000,
        consumers=5,
        log_utils=log_utils
    )

    stage.run()

if __name__ == '__main__':
    main()
