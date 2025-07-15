from threading import (
    Semaphore as _Semaphore,
    Condition as _Condition,
    Event as _Event
)


class Monitor:
    def __init__(self, buffer_size: int, timeout: int = 5):
        """
        Implementação de um monitor utilizando a biblioteca `threading`
        Args:
            buffer_size (int): Tamanho do buffer.
            timeout (int, optional): Quantidade de tempo que um thread espera até o mutex ser liberado.
        """
        self._buffer: list[tuple] = []
        self._buffer_size: int = buffer_size
        self._workers: list = []
        self._producers_online: int = 0
        self._mutex: _Semaphore = _Semaphore(1)
        self._full: _Condition = _Condition(self._mutex)
        self._empty: _Condition = _Condition(self._mutex)
        self._end_process: _Event = _Event()
        self._timeout: int = timeout

    def write(self, data: object):
        """
        Método para escrita na memória compartilhada.
        Args:
            data (object): Objeto genérico para ser escrito na memória.
        """
        """
        O thread tenta entrar na região crítica adquirindo a trava.
        Se não conseguir ele entra em espera até a trava ser liberada.
        """
        self._mutex.acquire()
        """
        Verifica se a memória está cheia.
        Se estiver, a thread entra em espera até ser notificado por um thread que consumiu da memória, ou até o tempo de espera acabar.
        """
        if len(self._buffer) == self._buffer_size:
            self._empty.wait(self._timeout)
        self._buffer.append(data)
        """
        Após escrever, outro thread que estava em espera para ler a memória compartilhada é notificado.
        Em seguida a trava é liberada.
        """
        self._full.notify()
        self._mutex.release()

    def read(self):
        """
        Método para leitura da memória compartilhada.

        Returns:
            object: Retorna um objetivo que estava escrito na memória.
        """
        data = None
        """
        O thread tenta entrar na região crítica adquirindo a trava.
        Se não conseguir ele entra em espera até a trava ser liberada.
        """
        self._mutex.acquire()
        """
        Verifica se a memória está vazia.
        Se estiver, a thread entra em estado de espera até ser notificado por um thread que escreveu na memória,
        ou até o tempo de espera acabar.
        """
        if len(self._buffer) == 0:
            self._full.wait(self._timeout)
        try:
            data = self._buffer.pop()
        except:
            """
            Se o thread não conseguir extrair dados da memória ele verifica se existe algum thread produtor
            ativo, se não tiver ele acorda todos os threads consumidores que estavam em espera.
            """
            if self._producers_online <= 0:
                self.stop_all_workers()
        finally:
            """
            Após consumir da memória, outro thread que estava em espera para escrever na memória compartilhada é notificado.
            Em seguida a trava é liberada.
            """
            try:
                self._empty.notify()
                self._mutex.release()
            except:
                pass
        
        return data
    
    def notify_all(self):
        """
        Método responsável por liberar a trava e notificar todos os threads que estão em espera.
        """
        try:
            self._mutex.release()
            self._full.notify_all()
            self._empty.notify_all()
        except:
            pass

    def stop_all_workers(self):
        """
        Esse método executa as rotinas necessárias para finalizar o processo com segurança.        
        """
        """
        Define o evento _end_process para True, para acordar o thread principal.
        """
        self._end_process.set()
        """
        Atualiza o atributo `_stop` de todos os threads para True, para que eles não entrem mais
        na região crítica e finalizar o programa.
        """
        for worker in self._workers:
            worker.stop()
        self.notify_all()

    def done(self):
        """
        Quando um thread produtor termina sua execução, ele entra na região crítica e decrementa o contador
        `_producers_online` para que threads consumidores saibam se devem esperar por mais dados.
        """
        self._mutex.acquire()
        self._producers_online -= 1
        self._mutex.release()

    def subscribe(self, worker):
        """
        Método para inscrever um thread no monitor para que o mesmo possa ser notificado
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