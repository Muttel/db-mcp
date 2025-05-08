from mysql.connector import connect, Error, MySQLConnection
from config.logger_config import LoggerFactory
from typing import Optional

# Set up logger for this module
logger = LoggerFactory.get_logger(__name__)

def connect_to_mysql(host:str, user:str, password:str, database:str) -> Optional[MySQLConnection]:
    """
    Establishes a connection to a MySQL database.

    Args:
        host (str): The host address of the MySQL server.
        user (str): The username to authenticate with.
        password (str): The password for the MySQL user.
        database (str): The name of the database to connect to.

    Returns:
        Optional[MySQLConnection]: A MySQLConnection object if the connection is successful, otherwise None.
    """
    try:
        connection = connect(
            host=host,
            user=user, 
            password=password, 
            database=database
        )
        if connection.is_connected():
            logger.info(f"Successfully connected to MySQL database '{database}' as user '{user}'")
            return connection
        else:
            logger.error(f"Failed to connect to MySQL database '{database}'")
            return None
    except Error as e:
        logger.error(f"Error while connecting to MySQL database '{database}': {str(e)}")
        return None