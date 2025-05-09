from config.logger_config import LoggerFactory
from mysqldb.services.connection import connect_to_mysql

from typing import Dict, List, Any
from mysql.connector.cursor import MySQLCursor
from mysql.connector.connection import MySQLConnection
from mysql.connector import Error

logger = LoggerFactory.get_logger(__name__)

def insert_row(host:str, user:str, password:str, database:str, table_name:str, data:Dict[str, Any]) -> bool :
    """
    Inserts a single row into the specified table.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to insert into.
        data (Dict[str, Any]): A dictionary containing column names and their values.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    try:
        connection:MySQLConnection = connect_to_mysql(host=host, user=user, password=password, database=database)
        if not connection:
            logger.error(f"Error connecting to database '{database}'")
            return False

        cursor:MySQLCursor = connection.cursor()
        columns = ','.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({values})"
        logger.info(f"Executing query: \n{query}")
        cursor.execute(query, list(data.values()))
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"Inserted {table_name} row")
        return True
    except Error as e:
        logger.error(f"Error inserting row '{table_name}': {e}")
        return False

def insert_multiple_rows(host: str, user: str, password: str, database: str, table_name: str, data: List[Dict[str, Any]]) -> bool:
    """
    Inserts multiple rows into the specified table.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to insert into.
        data (List[Dict[str, Any]]): A list of dictionaries, each representing a row to insert.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor()

        columns = ', '.join(data[0].keys())
        values = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        cursor.executemany(query, [tuple(row.values()) for row in data])
        connection.commit()
        logger.info(f"{len(data)} rows inserted into table '{table_name}'.")
        cursor.close()
        connection.close()
        return True
    except Error as e:
        logger.error(f"Error inserting multiple rows into table '{table_name}': {e}")
        return False



def update_rows(host: str, user: str, password: str, database: str, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
    """
    Updates rows in the specified table based on conditions.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to update.
        data (Dict[str, Any]): A dictionary containing columns and their new values.
        conditions (Dict[str, Any]): A dictionary containing conditions to match for updating.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor()

        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        cursor.execute(query, list(data.values()) + list(conditions.values()))
        connection.commit()
        logger.info(f"Rows updated in table '{table_name}' with conditions: {conditions}.")
        cursor.close()
        connection.close()
        return True
    except Error as e:
        logger.error(f"Error updating rows in table '{table_name}': {e}")
        return False


def delete_rows(host: str, user: str, password: str, database: str, table_name: str, conditions: Dict[str, Any]) -> bool:
    """
    Deletes rows from the specified table based on conditions.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The name of the table to delete from.
        conditions (Dict[str, Any]): A dictionary containing conditions to match for deletion.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database)
        cursor: MySQLCursor = connection.cursor()

        where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"

        cursor.execute(query, list(conditions.values()))
        connection.commit()
        logger.info(f"Rows deleted from table '{table_name}' with conditions: {conditions}.")
        cursor.close()
        connection.close()
        return True
    except Error as e:
        logger.error(f"Error deleting rows from table '{table_name}': {e}")
        return False
