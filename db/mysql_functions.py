"""
this module creates functions for connection to MySQL database
"""
import os

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.abstracts import (MySQLConnectionAbstract,
                                       MySQLCursorAbstract)

load_dotenv()


def mysql_connect() -> tuple[MySQLConnectionAbstract, MySQLCursorAbstract]:
    """
    create a MySQL connection using environmental variables
    :return: connection and its cursor as a tuple
    """
    connection = mysql.connector.connect(user=os.environ['mysql_user'],
                                         password=os.environ['mysql_password'],
                                         host=os.environ['mysql_host'],
                                         database=os.environ['mysql_database'])
    cursor = connection.cursor()
    return connection, cursor


if __name__ == '__main__':
    connection, cursor = mysql_connect()
    cursor.execute('show tables')
    print(cursor.fetchall())
    cursor.close()
    connection.close()
