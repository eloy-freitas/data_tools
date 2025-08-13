import json
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import Engine as _Engine


class PostgresConnectionFactory():
    def __init__(self) -> None:
        super().__init__()
    
    def read_file(self, conn_id: str, file_path:str) -> dict[str, str]:
        data = None
        
        try:
            with open(file_path) as file:
                data = json.load(file)
        except IOError as e:
            raise IOError(f"Connection file {file_path} not found:\n{e}") from e

        result = data[conn_id]
        
        if len(result) == 0:
            raise KeyError(f"Connection id {conn_id} not found")
        
        return result
    
    def create_connection_url(self, connection_dict: dict[str, str]) -> str:
        user = connection_dict['user']
        password = connection_dict['password']
        host = connection_dict['host']
        port = connection_dict['port']
        database = connection_dict['database']
        
        connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        return connection_str
    
    def create_engine_by_file(self, conn_id:str, file_path:str) -> _Engine:
        conn_dict = self.read_file(conn_id, file_path)
        url = self.create_connection_url(conn_dict)
        
        return _create_engine(url)
    