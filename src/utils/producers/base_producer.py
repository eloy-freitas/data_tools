from abc import ABC, abstractmethod


class BaseProducer(ABC):
    """
    Interface de uma classe produtora de dados.
    """

    @abstractmethod
    def extract(self):...
    """
    Assinatura do método para extração de dados.
    """