from config.mcp_config import mcp
import controllers
from mysqldb.services.ddl import create_table, drop_table, show_indexes, create_index
from mysqldb.services.schema import get_table_description
import json

host = "localhost"
user = "root"
password = "tirthraj07"
database = "dbms_project"
table_name = "temp"

def test():
    ...

if __name__ == "__main__":
    # mcp.run(transport='stdio')
    test()