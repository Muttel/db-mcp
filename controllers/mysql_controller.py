from config.mcp_config import mcp
from mysqldb.services.schema import get_schema,get_tables, get_table_description
from mysqldb.services.reads import get_all_rows, get_filtered_rows, get_sorted_rows, get_limited_rows, get_distinct_values, get_aggregated_data, get_grouped_data, execute_custom_query
from typing import Optional, Dict, List, Any

@mcp.tool()
def get_mysql_tables(host:str, user:str, password:str, database:str) -> Optional[List[str]] :
    """
    Retrieves the list of table names in a MySQL database.

    Args:
        host (str): The host address of the MySQL server.
        user (str): The username to authenticate with.
        password (str): The password for the MySQL user.
        database (str): The name of the database to fetch tables from.

    Returns:
        Optional[List[str]]: A list of table names if successful, otherwise None.
    """
    return get_tables(host=host, user=user, password=password, database=database)

@mcp.tool()
def get_mysql_schema(host:str, user:str, password:str, database:str) -> Optional[Dict[str, list]] :
    """
    Retrieves the schema of a given MySQL database as a dictionary.

    Args:
        host (str): The host address of the MySQL server.
        user (str): The username to authenticate with.
        password (str): The password for the MySQL user.
        database (str): The name of the database to retrieve the schema from.

    Returns:
        Optional[Dict[str, list]]: A dictionary where keys are table names and values are lists of column definitions.
        Returns None if the connection or table retrieval fails.
    """
    return get_schema(host=host, user=user, password=password, database=database)

@mcp.tool()
def get_mysql_table_description(host:str, user:str, password:str, database:str, table_name:str) -> Optional[Dict[str, list]] :
    """
    Retrieves the schema of a specific table from a given MySQL database.

    Args:
        host (str): The host address of the MySQL server.
        user (str): The username to authenticate with.
        password (str): The password for the MySQL user.
        database (str): The name of the database containing the table.
        table_name (str): The name of the table to retrieve the schema from.

    Returns:
        Optional[Dict[str, list]]: A dictionary where the key is the table name and the value is a list of column definitions.
        Returns None if the connection or schema retrieval fails.
    """
    return get_table_description(host=host, user=user, password=password, database=database, table_name=table_name)

@mcp.tool()
def mysql_get_all_rows(host:str, user:str, password:str, database:str, table_name:str) -> Optional[List[Dict]]:
    """
    Retrieves all rows from the specified table in the given database.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The name of the database.
        table_name (str): The name of the table to fetch data from.

    Returns:
        Optional[List[Dict]]: A list of dictionaries where each dictionary represents a row from the table.
        Returns None if an error occurs or the table is empty.
    """
    return get_all_rows(host=host, user=user, password=password, database=database, table_name=table_name)

@mcp.tool()
def mysql_get_filtered_rows(host: str, user: str, password: str, database: str, table_name: str, filters: Dict[str, Any]) -> Optional[List[Dict]]:
    """
    Retrieves rows from the specified table in the given database based on filter criteria.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The name of the database.
        table_name (str): The name of the table to fetch data from.
        filters (Dict[str, Any]): A dictionary where keys are column names and values are the filtering criteria.

    Returns:
        Optional[List[Dict]]: A list of dictionaries where each dictionary represents a row from the table.
        Returns None if an error occurs or no data matches the filters.
    """
    return get_filtered_rows(host, user, password, database, table_name, filters)

@mcp.tool()
def mysql_get_sorted_rows(host: str, user: str, password: str, database: str, table_name: str, sort_by: str, order: str = 'ASC')  -> Optional[List[Dict]]:
    """
    Retrieves sorted rows from the specified table.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The table name.
        sort_by (str): The column to sort by.
        order (str): The sort order ('ASC' or 'DESC'). Default is 'ASC'.

    Returns:
        Optional[List[Dict]]: Sorted rows as a list of dictionaries, or None on error.
    """
    return get_sorted_rows(host, user, password, database, table_name, sort_by, order)

@mcp.tool()
def mysql_get_limited_rows(host: str, user: str, password: str, database: str, table_name: str, limit: int, offset: int = 0)  -> Optional[List[Dict]]:
    """
    Retrieves a limited number of rows from the specified table.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The table name.
        limit (int): The number of rows to fetch.
        offset (int): The starting point for fetching rows. Default is 0.

    Returns:
        Optional[List[Dict]]: Limited rows as a list of dictionaries, or None on error.
    """
    return get_limited_rows(host, user, password, database, table_name, limit, offset)

@mcp.tool()
def mysql_get_distinct_values(host: str, user: str, password: str, database: str, table_name: str, column: str) -> Optional[List[Any]]:
    """
    Retrieves distinct values from a specific column in the given table.

    Args:
        host (str): The database host.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
        table_name (str): The table name.
        column (str): The column name.

    Returns:
        Optional[List[Any]]: A list of distinct values or None on error.
    """
    return get_distinct_values(host, user, password, database, table_name, column)

@mcp.tool()
def mysql_get_aggregated_data(host: str, user: str, password: str, database: str, table_name: str, aggregation: str, column: str) -> Optional[Any]:
    """
    Retrieves aggregated data from a specified column in the given table.

    Args:
        aggregation (str): The aggregation function (e.g., COUNT, SUM, AVG).
        column (str): The column name.

    Returns:
        Optional[Any]: The result of the aggregation or None on error.
    """
    return get_aggregated_data(host, user, password, database, table_name, aggregation, column)

@mcp.tool()
def mysql_get_grouped_data(host: str, user: str, password: str, database: str, table_name: str, group_by: str, aggregation: str, column: str) -> Optional[Dict[str, Any]]:
    """
    Groups data by a specified column and applies an aggregation function.

    Args:
        group_by (str): The column to group by.
        aggregation (str): The aggregation function (e.g., SUM, AVG).
        column (str): The column to aggregate.

    Returns:
        Optional[Dict[str, Any]]: A dictionary with group values as keys and aggregated data as values.
    """
    return get_grouped_data(host, user, password, database, table_name, group_by, aggregation, column)

# @mcp.tool()
# def mysql_execute_custom_query(host: str, user: str, password: str, database: str, query: str) -> Optional[List[Dict]]:
#     """
#     Executes a custom SQL query and returns the result as a list of dictionaries.

#     Args:
#         host (str): The database host.
#         user (str): The database user.
#         password (str): The database password.
#         database (str): The database name.
#         query (str): The SQL query to execute.

#     Returns:
#         Optional[List[Dict]]: The result of the query as a list of dictionaries, or None if an error occurs.
#     """
#     return execute_custom_query(host, user, password, database, query)