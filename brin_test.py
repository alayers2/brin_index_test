from io import StringIO

import psycopg2
import pandas as pd
import numpy as np


def _get_conn():
    return psycopg2.connect(
        host="db_container",
        database="test_db",
        user="postgres",
        password="password"
    )


def copy_from_stringio(conn, df, table):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with conn:
        with conn.cursor() as curs:
            copy_sql = """
               COPY {table}
                   FROM STDIN
               WITH (
                   FORMAT CSV,
                   DELIMITER ','
               );
               """.format(table=table)
            curs.copy_expert(copy_sql, buffer)


def _create_table(conn, table_name):

    with conn:
        with conn.cursor() as curs:
            curs.execute(
                f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} (
                    hourly_timestamp timestamptz,
                    cell_id int8,
                    temperature float8,
                    rainfall float8
                );
                """
            )


def populate_hourly_data_one_cell_seq():
    timestamps = pd.date_range(start='19890101', end='20190101', freq='H')

    df = pd.DataFrame({
        'hourly_timestamp': timestamps,
        'cell_id': 1,
        'temperature': np.random.uniform(0, 100, timestamps.shape),
        'rainfall': np.random.uniform(0, 4, timestamps.shape)
    })

    conn = _get_conn()
    table_name = 'small_dataset_one_cell'
    _create_table(conn, table_name)
    copy_from_stringio(conn, df, table_name)
    conn.close()


def populate_hourly_data_multi_cell_seq():

    conn = _get_conn()
    table_name = 'large_dataset_multi_cell'
    _create_table(conn, table_name)

    timestamps = pd.date_range(start='1989-01-01', end='2019-01-01', freq='H')
    cell_ids = np.arange(1000)

    for timestamp in timestamps:
        df = pd.DataFrame({
            'hourly_timestamp': timestamp,
            'cell_id': cell_ids,
            'temperature': np.random.uniform(0, 100, cell_ids.shape),
            'rainfall': np.random.uniform(0, 4, cell_ids.shape)
        })

        copy_from_stringio(conn, df, table_name)
    conn.close()


def populate_hourly_data_multi_cell_shuffle():
    conn = _get_conn()
    table_name = 'large_dataset_multi_cell_shuffle'
    _create_table(conn, table_name)

    timestamps = pd.date_range(start='1989-01-01', end='2019-01-01', freq='H').to_list()
    np.random.shuffle(timestamps)

    cell_ids = np.arange(1000)

    for timestamp in timestamps:
        df = pd.DataFrame({
            'hourly_timestamp': timestamp,
            'cell_id': cell_ids,
            'temperature': np.random.uniform(0, 100, cell_ids.shape),
            'rainfall': np.random.uniform(0, 4, cell_ids.shape)
        })

        copy_from_stringio(conn, df, table_name)
    conn.close()


if __name__ == '__main__':
    populate_hourly_data_one_cell_seq()
    populate_hourly_data_multi_cell_seq()
    populate_hourly_data_multi_cell_shuffle()
