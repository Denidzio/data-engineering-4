#!/usr/bin/env python
""" \
    Лабораторна робота №4
    Виконав студент 543 групи Лунгу Денис
"""

from utils import create_db_connection, process_table


db_options = {
    "host": "postgres",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
}

ordered_tables = ["accounts", "products", "transactions"]


def main():
    connection = create_db_connection(db_options)
    [process_table(table, connection) for table in ordered_tables]
    connection.close()


if __name__ == "__main__":
    main()
