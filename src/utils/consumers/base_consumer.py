from abc import ABC, abstractmethod

class BaseConsumer(ABC):
    
    @abstractmethod
    def load(self):...