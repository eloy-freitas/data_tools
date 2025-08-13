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
        self._insert_query_available: _Condition = _Condition(self._mutex)
        self._end_process: _Event = _Event()
        self._timeout: int = timeout
        self._insert_query = None

    def write(self, data: object):
        with self._mutex:
            if len(self._buffer) == self._buffer_size:
                self._empty.wait()
            self._buffer.append(data)
            self._full.notify()

    def read(self):
        data = None
        with self._mutex:
            if len(self._buffer) == 0 and self._producers_online > 0:
                self._full.wait()
            if self._buffer:
                data = self._buffer.pop()
                self._empty.notify()
            elif self._producers_online <= 0:
                self.stop_all_workers()
            
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

        self._end_process.set()

    def done(self):
        with self._mutex:
            self._producers_online -= 1

    def subscribe(self, worker):
        if worker.is_producer:
            self._producers_online += 1
        self._workers.append(worker)
            
    def start(self):
        for worker in self._workers:
            worker.start()

    def set_insert_query(self, query: str):
        with self._mutex:
            self._insert_query = query
            self._insert_query_available.notify_all()

    def get_insert_query(self):
        with self._mutex:
            if not self._insert_query:
                self._insert_query_available.wait()
            query = self._insert_query

        return query

    def wait_for_completion(self):
        self._end_process.wait()