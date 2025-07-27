from src.templates.stages.template_stage_multithread import StageMultiThread
from src.connection.postgres_connection_factory import PostgresConnectionFactory


def main():
    query = "select * from tabela_2"

    table_name_target = 'tabela_2_target'

    postgres_conn_factory = PostgresConnectionFactory()

    conn_id = 'dbdw'

    path_conn_file = 'src/resources/postgres_connections.json'

    db_conn = postgres_conn_factory.create_engine_by_file(
        conn_id=conn_id, 
        file_path=path_conn_file
    )

    stage = StageMultiThread(
        query=query,
        table_name_taget=table_name_target,
        conn_input=db_conn,
        conn_output=db_conn,
        yield_per=40000,
        consumers=5
    )

    stage.start()

if __name__ == '__main__':
    main()
