import logging
from logging import Logger


class LogUtils:
    def __init__(self):
        """
        Initialize the logging configuration with INFO level, a custom message format, and a specific date format.
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%d/%m/%Y %H:%M:%S'
        )

    def get_logger(self, name) -> Logger:
        """
        Return a logger instance associated with the specified name.
        
        Parameters:
            name (str): The name of the logger to retrieve.
        
        Returns:
            Logger: A logger configured with the global logging settings.
        """
        return logging.getLogger(name)
