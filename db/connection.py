import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()


def get_connection(database_override=None):
    server = os.getenv("AZURE_SQL_SERVER")
    database = database_override or os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")
    port = os.getenv("AZURE_SQL_PORT", "1433")

    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )

    conn = pyodbc.connect(connection_string)
    return conn
