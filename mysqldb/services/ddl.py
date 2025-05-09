from mysqldb.services.connection import connect_to_mysql
from config.logger_config import LoggerFactory

from typing import Optional, Dict, List, Any
from mysql.connector import Error, MySQLConnection
from mysql.connector.cursor import MySQLCursor

logger = LoggerFactory.get_logger(__name__)

def create_table(host:str, user:str, password:str, database:str, table_name:str, columns: Dict[str, str], options: Optional[Dict[str, str]] = None) -> bool:
    """
    Creates a table with the specified columns and options.
    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to create.
        columns (Dict[str, str]): A dictionary of column names and their data types.
        options (Optional[Dict[str, str]]): Additional options like PRIMARY KEY, AUTO_INCREMENT, UNIQUE, NOT NULL.
    Returns:
        bool: True if the table was created successfully, False otherwise.
    """
    try:
        connection : MySQLConnection = connect_to_mysql(host=host, user=user, password=password,database=database)
        cursor : MySQLCursor = connection.cursor(dictionary=True)
        column_definitions = []
        for column_name, column_type in columns.items():
            definition = f"{column_name} {column_type}"
            if options and options.get(column_name) is not None:
                definition += f" {options.get(column_name)}"
            column_definitions.append(definition)

        query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
        logger.debug(f"Executing query: \n{query}")
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"Table '{table_name}' created successfully.")
        return True
    except Error as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        return False

def drop_table(host: str, user: str, password: str, database: str, table_name: str) -> bool:
    """
    Drops the specified table.
    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to drop.
    Returns:
        bool: True if the table was dropped successfully, False otherwise.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor()
        query = f"DROP TABLE IF EXISTS {table_name}"
        logger.debug(f"Executing query: \n{query}")
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"Table '{table_name}' dropped successfully.")
        return True
    except Error as e:
        logger.error(f"Error dropping table '{table_name}': {e}")
        return False

def show_indexes(host: str, user: str, password: str, database: str, table_name: str) -> Optional[List[Dict[str, str]]]:
    """
    Retrieves all indexes from a given table.
    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to retrieve indexes from.
    Returns:
        Optional[List[Dict[str, str]]]: A list of dictionaries containing index information, or None if an error occurs.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor(dictionary=True)

        query = f"SHOW INDEX FROM {table_name}"
        logger.debug(f"Executing query: \n{query}")
        cursor.execute(query)
        indexes = cursor.fetchall()
        logger.info(f"Indexes retrieved from table '{table_name}': {indexes}")
        cursor.close()
        connection.close()
        return indexes
    except Error as e:
        logger.error(f"Error retrieving indexes from table '{table_name}': {e}")
        return None

def create_index(host: str, user: str, password: str, database: str, table_name: str, index_name: str, columns: List[str], unique: bool = False) -> bool:
    """
    Creates an index on the specified columns of a table.
    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The table to create the index on.
        index_name (str): The name of the index.
        columns (List[str]): A list of column names to be indexed.
        unique (bool): If True, creates a UNIQUE index. Defaults to False.
    Returns:
        bool: True if the index was created successfully, False otherwise.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor()

        index_type = "UNIQUE" if unique else ""
        query = f"CREATE {index_type} INDEX {index_name} ON {table_name} ({', '.join(columns)})"
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"Index '{index_name}' created successfully on table '{table_name}'.")
        return True
    except Error as e:
        logger.error(f"Error creating index '{index_name}': {e}")
        return False
