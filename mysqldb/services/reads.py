from mysqldb.services.connection import connect_to_mysql
from config.logger_config import LoggerFactory

from typing import Dict, Optional, List, Any
from mysql.connector.cursor import MySQLCursor
from mysql.connector.connection import MySQLConnection
from mysql.connector import Error

logger = LoggerFactory.get_logger(__name__)

def get_all_rows(host: str, user: str, password: str, database: str, table_name: str, port: int = 3307) -> Optional[List[Dict]]:
    """
    Retrieves all rows from the specified table in the given database.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host=host, user=user, password=password, database=database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)
        rows: List[Dict] = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(f"Error occurred while fetching data from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_filtered_rows(
    host: str, user: str, password: str, database: str, table_name: str, filters: Dict[str, Any], port: int = 3307
) -> Optional[List[Dict]]:
    """
    Retrieves rows from the specified table in the given database based on filter criteria.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host=host, user=user, password=password, database=database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        where_clauses = [f"`{key}` = %s" for key in filters.keys()]
        where_statement = " AND ".join(where_clauses)
        query = f"SELECT * FROM {table_name} WHERE {where_statement};"
        values = list(filters.values())
        cursor.execute(query, values)
        rows: List[Dict] = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(f"Error occurred while fetching filtered data from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_sorted_rows(
    host: str, user: str, password: str, database: str, table_name: str, sort_by: str, order: str = 'ASC', port: int = 3307
) -> Optional[List[Dict]]:
    """
    Retrieves sorted rows from the specified table.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} ORDER BY `{sort_by}` {order};"
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(f"Error fetching sorted rows from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_limited_rows(
    host: str, user: str, password: str, database: str, table_name: str, limit: int, offset: int = 0, port: int = 3307
) -> Optional[List[Dict]]:
    """
    Retrieves a limited number of rows from the specified table.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s;"
        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(f"Error fetching limited rows from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_distinct_values(
    host: str, user: str, password: str, database: str, table_name: str, column: str, port: int = 3307
) -> Optional[List[Any]]:
    """
    Retrieves distinct values from a specific column in the given table.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor()
        query = f"SELECT DISTINCT `{column}` FROM {table_name};"
        cursor.execute(query)
        values = [row[0] for row in cursor.fetchall()]
        return values
    except Error as e:
        logger.error(f"Error fetching distinct values from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_aggregated_data(
    host: str, user: str, password: str, database: str, table_name: str, aggregation: str, column: str, port: int = 3307
) -> Optional[Any]:
    """
    Retrieves aggregated data from a specified column in the given table.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor()
        query = f"SELECT {aggregation}(`{column}`) FROM {table_name};"
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None
    except Error as e:
        logger.error(f"Error in aggregation on '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_grouped_data(
    host: str, user: str, password: str, database: str, table_name: str, group_by: str, aggregation: str, column: str, port: int = 3307
) -> Optional[Dict[str, Any]]:
    """
    Groups data by a specified column and applies an aggregation function.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        query = f"SELECT `{group_by}`, {aggregation}(`{column}`) AS aggregate FROM {table_name} GROUP BY `{group_by}`;"
        cursor.execute(query)
        results = cursor.fetchall()
        return {row[group_by]: row['aggregate'] for row in results}
    except Error as e:
        logger.error(f"Error fetching grouped data from '{table_name}': {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def execute_custom_query(host: str, user: str, password: str, database: str, query: str, port: int = 3307) -> Optional[List[Dict]]:
    """
    Executes a custom SQL query and returns the result as a list of dictionaries.
    """
    connection = None
    cursor = None
    try:
        connection: MySQLConnection = connect_to_mysql(host, user, password, database, port=port)
        if not connection:
            return None
        cursor: MySQLCursor = connection.cursor(dictionary=True)
        logger.info(f"Executing custom query: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(f"Error executing custom query: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()