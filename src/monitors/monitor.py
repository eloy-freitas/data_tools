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
        self._insert_query_avaliable: _Condition = _Condition(self._mutex)
        self._end_process: _Event = _Event()
        self._timeout: int = timeout
        self._insert_query = None

    def write(self, data: object):
        """
        Adds an item to the buffer, blocking if the buffer is full until space becomes available.
        
        Parameters:
            data (object): The item to add to the buffer.
        """
        self._mutex.acquire()
        if len(self._buffer) == self._buffer_size:
            self._empty.wait()
        self._buffer.append(data)
        self._full.notify()
        self._mutex.release()

    def read(self):
        """
        Retrieve and remove an item from the buffer, blocking if the buffer is empty and producers are still active.
        
        Returns:
            The next item from the buffer, or None if the buffer is empty and no producers remain.
        """
        data = None
        self._mutex.acquire()
        if len(self._buffer) == 0 and self._producers_online > 0:
            self._full.wait()
        try:
            data = self._buffer.pop()
        except:
            if self._producers_online <= 0:
                self.stop_all_workers()

        try:
            self._empty.notify()
            self._mutex.release()
        except:
            pass
            
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

    def done(self):
        """
        Mark one producer as finished by decrementing the count of active producers.
        """
        self._mutex.acquire()
        self._producers_online -= 1
        self._mutex.release()

    def subscribe(self, worker):
        """
        Registers a worker with the monitor and increments the active producer count if applicable.
        
        Parameters:
            worker: The worker instance to subscribe. If the worker is a producer, it increases the count of active producers.
        """
        if worker._is_producer:
            self._producers_online += 1
        self._workers.append(worker)
            
    def start(self):
        """
        Start all subscribed worker threads managed by the monitor.
        """
        for worker in self._workers:
            worker.start()

    def set_insert_query(self, query: str):
        """
        Set the insert query string and notify all threads waiting for it to become available.
        
        Parameters:
            query (str): The insert query string to set.
        """
        self._mutex.acquire()
        self._insert_query = query
        self._insert_query_avaliable.notify_all()
        self._mutex.release()

    def get_insert_query(self):
        """
        Wait until the insert query string is set and then return it.
        
        Returns:
            str: The insert query string once it becomes available.
        """
        self._mutex.acquire()
        if not self._insert_query:
            self._insert_query_avaliable.wait()
        
        query = self._insert_query
        self._mutex.release()

        return query