from .connection_factory import ConnectionFactory
import json
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import Engine as _Engine


class PostgresConnectionFactory(ConnectionFactory):
    def __init__(self) -> None:
        """
        Initialize a new instance of PostgresConnectionFactory.
        """
        super().__init__()
    
    def read_file(self, conn_id: str, file_path:str) -> dict[str, str]:
        """
        Read a JSON file and retrieve the connection details for a specified connection ID.
        
        Parameters:
            conn_id (str): The key identifying the desired connection in the JSON file.
            file_path (str): Path to the JSON file containing connection information.
        
        Returns:
            dict[str, str]: A dictionary with connection parameters for the specified connection ID.
        
        Raises:
            IOError: If the file cannot be read.
            KeyError: If the connection ID does not exist or contains no data.
        """
        data = None
        
        try:
            with open(file_path) as file:
                data = json.load(file)
        except IOError as e:
            raise IOError(f"Falha ao ler arquivo:\n{e}")

        result = data[conn_id]
        
        if len(result) == 0:
            raise KeyError(f"Nome da conexão não existe")
        
        return result
    
    def create_connection_url(self, connection_dict: dict[str, str]) -> str:
        """
        Construct a PostgreSQL connection URL from the provided connection details.
        
        Parameters:
        	connection_dict (dict[str, str]): Dictionary containing 'user', 'password', 'host', 'port', and 'database' keys.
        
        Returns:
        	str: A PostgreSQL connection URL in the format 'postgresql+psycopg2://user:password@host:port/database'.
        """
        user = connection_dict['user']
        password = connection_dict['password']
        host = connection_dict['host']
        port = connection_dict['port']
        database = connection_dict['database']
        
        connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        return connection_str
    
    def create_engine_by_file(self, conn_id:str, file_path:str) -> _Engine:
        """
        Create a SQLAlchemy engine for a PostgreSQL database using connection details loaded from a JSON file.
        
        Parameters:
            conn_id (str): The identifier for the connection details within the JSON file.
            file_path (str): Path to the JSON file containing connection configurations.
        
        Returns:
            _Engine: A SQLAlchemy engine instance configured for the specified PostgreSQL connection.
        """
        conn_dict = self.read_file(conn_id, file_path)
        url = self.create_connection_url(conn_dict)
        
        return _create_engine(url)
    
    def create_engine(self, conn_dict:dict) -> _Engine:
        """
        Create a SQLAlchemy engine for a PostgreSQL database using the provided connection details.
        
        Parameters:
        	conn_dict (dict): Dictionary containing connection parameters such as user, password, host, port, and database.
        
        Returns:
        	_Engine: A SQLAlchemy engine instance configured for the specified PostgreSQL connection with SQL statement logging enabled.
        """
        url = self.create_connection_url(conn_dict)
        
        return _create_engine(url, echo=True)
