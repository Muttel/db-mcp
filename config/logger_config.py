import os
import logging
from logging import Logger

# Define the directory where log files will be stored
LOGGING_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOGGING_DIR, exist_ok=True)

class LoggerFactory:
    """
    A factory class to create and configure loggers with a consistent setup.
    """

    @staticmethod
    def get_logger(name: str) -> Logger:
        """
        Retrieves a logger with the specified name, configuring it if necessary.

        Args:
            name (str): The name of the logger.

        Returns:
            Logger: Configured logger instance.
        """
        logger = logging.getLogger(f'log_namespace.{name}')
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            # Create file handler which logs debug and higher level messages
            file_path = os.path.join(LOGGING_DIR, f'{name}.log')
            file_handler = logging.FileHandler(file_path)
            file_handler.setLevel(logging.DEBUG)

            # Create console handler with a higher log level
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)

            # Create formatter and add it to the handlers
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s:%(name)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add the handlers to the logger
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

            # Prevent log messages from being propagated to the root logger
            logger.propagate = False

        return logger
