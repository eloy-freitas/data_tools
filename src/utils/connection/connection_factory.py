from abc import ABC, abstractmethod

class ConnectionFactory(ABC):
    
    @abstractmethod
    def create_engine(self):
        pass