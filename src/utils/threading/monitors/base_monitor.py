from abc import (
    ABC as _ABC, 
    abstractmethod as _abstractclassmethod
)


class BaseMonitor(_ABC):
    def __init__(self, buffer_size:int):
        """
        Classe abstrata para implementação de um monitor.
        Monitores são classes que possuem métodos especializados para manipulação de memória 
        compartilhada visando garantir a exclusão mutua com segurança.

        Args:
            buffer_size (int): Tamanho da memória compartilhada.
        """
        self._buffer: list[tuple] = []
        self._buffer_size: int = buffer_size
        self._workers: list = []
        self._producers_online: int = 0
        
    @_abstractclassmethod
    def write(self, data: object) -> None:...
    """
    Assinatura do método para escrita na memória compartilhada.
    """

    @_abstractclassmethod
    def read(self) -> object:...
    """
    Assinatura do método para leitura na memória compartilhada.
    """

    @_abstractclassmethod
    def done(self):...
    """
    Assinatura do método para sinalizar que os threads terminaram o processamento.
    """

    @_abstractclassmethod
    def stop_all_workers(self):...
    """
    Assinatura do método para execução quando um erro ocorre durante o processamento.
    """

    @_abstractclassmethod
    def notify_all(self):...
    """
    Assinatura do método para arcordar todos os threads que estão em estado de wait.
    """

    def subscribe(self, worker):
        """
        Método para inscrever um thread no monitor para que o mesmo possa receber notificado
        quando ocorrer algum evento.
        """
        if worker._is_producer:
            self._producers_online += 1
        self._workers.append(worker)
            
    def start(self):
        """
        Método para iniciar a execução de todos os threads inscritos.
        """
        for worker in self._workers:
            worker.start()
    