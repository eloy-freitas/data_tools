from threading import (
    Semaphore as _Semaphore,
    Condition as _Condition,
    Event as _Event
)


class Monitor:
    def __init__(self, buffer_size: int, timeout: int = 5):
        """
        Initialize a Monitor instance for coordinating producer and consumer threads with a bounded buffer.
        
        Parameters:
            buffer_size (int): Maximum number of items the buffer can hold.
            timeout (int, optional): Timeout in seconds for thread synchronization operations. Defaults to 5.
        """
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
        """
        Retrieve and remove an item from the buffer, blocking if the buffer is empty and producers are still active.
        
        Returns:
            The next item from the buffer, or None if the buffer is empty and no producers remain.
        """
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
        """
        Notify all threads waiting on buffer state changes and release the mutex.
        
        This method releases the internal mutex and wakes all threads waiting on both the full and empty condition variables. Exceptions during this process are silently ignored.
        """
        try:
            self._mutex.release()
            self._full.notify_all()
            self._empty.notify_all()
        except:
            pass

    def stop_all_workers(self):        
        """
        Stops all subscribed workers, notifies waiting threads, and signals the end of processing.
        """
        for worker in self._workers:
            worker.stop()
        self.notify_all()

        self._end_process.set()

    def producer_end_process(self):
        with self._mutex:
            self._producers_online -= 1

    def subscribe(self, worker):
        if worker.is_producer:
            self._producers_online += 1
        self._workers.append(worker)
            
    def start(self):
        """
        Start all subscribed worker threads managed by the monitor.
        """
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
    
    def signal_end_process(self): 
        self._end_process.set()
        