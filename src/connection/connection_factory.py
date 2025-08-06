from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine as _Engine

class ConnectionFactory(ABC):
    
    @abstractmethod
    def create_engine(self) -> _Engine:
        """
        Create and return a new SQLAlchemy Engine instance.
        
        Returns:
            Engine: A SQLAlchemy Engine object for database connections.
        """
        pass