import os
import csv
from psycopg2 import sql, connect
from pathlib import Path


def check_if_db_exists(db_options, connection):
    db_name = db_options.get("database")
    db_owner = db_options.get("user")

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'"
        )

        does_db_exist = cursor.fetchone()

        if not does_db_exist:
            cursor.execute(
                f"""
                CREATE DATABASE {db_name}
                    WITH
                    OWNER = {db_owner}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False;
                           """
            )


def create_db_connection(db_options):
    connection = connect(**db_options)
    check_if_db_exists(db_options, connection)
    return connection


def process_table(table_name: str, connection):
    create_table_by_name(table_name, connection)
    insert_data_from_csv(table_name, connection)


def create_table_by_name(table_name: str, connection):
    schema_path = f"schema/{table_name}.sql"
    does_schema_exist = os.path.isfile(schema_path)

    if not does_schema_exist:
        raise Exception("Виникла помилка: скрипт для таблиці '{table_name}' відсутній.")

    ddl_script = Path(schema_path).read_text()

    with connection.cursor() as cursor:
        cursor.execute(ddl_script)

    connection.commit()
    cursor.close()


def insert_data_from_csv(table_name: str, connection):
    csv_path = f"data/{table_name}.csv"
    does_csv_exist = os.path.isfile(csv_path)

    if not does_csv_exist:
        raise Exception("Виникла помилка: дані для таблиці '{table_name}' відсутні.")

    with connection.cursor() as cursor:
        with open(csv_path, "r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)
            prepared_rows = [[value.strip() for value in row] for row in csv_reader]

            cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name)))

            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(
                    map(lambda col: sql.Identifier(col.strip()), header)
                ),
                sql.SQL(", ").join(sql.Placeholder() * len(header)),
            )

            cursor.executemany(insert_query, prepared_rows)

    connection.commit()
    cursor.close()
