from threading import (
    Semaphore as _Semaphore,
    Condition as _Condition,
    Event as _Event
)


class Monitor:
    def __init__(self, buffer_size: int, timeout: int = 5):
        self._buffer: list[tuple] = []
        self._buffer_size: int = buffer_size
        self._workers: list = []
        self._producers_online: int = 0
        self._mutex: _Semaphore = _Semaphore(1)
        self._full: _Condition = _Condition(self._mutex)
        self._empty: _Condition = _Condition(self._mutex)
        self._insert_query_avaliable: _Condition = _Condition(self._mutex)
        self._end_process: _Event = _Event()
        self._timeout: int = timeout
        self._insert_query = None

    def write(self, data: object):
        self._mutex.acquire()
        # verifica se o buffer esta cheio
        # espera ate liberar espaço da memória
        if len(self._buffer) == self._buffer_size:
            self._empty.wait()
        self._buffer.append(data)
        self._full.notify()
        self._mutex.release()

    def read(self):
        data = None
        # sessão crítica
        self._mutex.acquire()
        # verifica se o buffer está vazio e se existe algum produtor online
        if len(self._buffer) == 0 and self._producers_online > 0:
            self._full.wait()
        try:
            data = self._buffer.pop()
        except:
            # se o buffer estiver vazio e nenhum produtor estiver online
            # todos os workes devem parar
            if self._producers_online <= 0:
                self.stop_all_workers()

        try:
            self._empty.notify()
            self._mutex.release()
        except:
            pass
            
        return data
    
    def notify_all(self):
        try:
            self._mutex.release()
            self._full.notify_all()
            self._empty.notify_all()
        except:
            pass

    def stop_all_workers(self):        
        for worker in self._workers:
            worker.stop()
        self.notify_all()

        # libera processo que espera pelo evento
        self._end_process.set()

    def done(self):
        # atualização contador de workers online
        self._mutex.acquire()
        self._producers_online -= 1
        self._mutex.release()

    def subscribe(self, worker):
        # inscrição dos workers
        if worker._is_producer:
            self._producers_online += 1
        self._workers.append(worker)
            
    def start(self):
        # inicia a execução de todos os threads inscritos.
        for worker in self._workers:
            worker.start()

    def set_insert_query(self, query: str):
        self._mutex.acquire()
        self._insert_query = query
        self._insert_query_avaliable.notify_all()
        self._mutex.release()

    def get_insert_query(self):
        self._mutex.acquire()
        if not self._insert_query:
            self._insert_query_avaliable.wait()
        
        query = self._insert_query
        self._mutex.release()

        return query