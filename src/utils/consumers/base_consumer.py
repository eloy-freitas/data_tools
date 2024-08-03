from abc import ABC, abstractmethod


class BaseConsumer(ABC):
    """
    Interface de uma classe consumidora de dados.
    """    
    @abstractmethod
    def load(self):...