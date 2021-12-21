import os
import psycopg2  # pip install psycopg2

# need to have add sql script: \postgres\test.sql

pg_creds = {
    'host': '192.168.88.52',
    'port': 5432,
    'database': 'postgres',
    'user': 'pguser',
    'password': 'secret'
}


def read_jdbc():
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        # previously in Postgres DB (by pg_creds) create table test and populate data
        cursor.execute("SELECT * FROM test;")
        result = cursor.fetchall()  # fetchone(), fetchmany(size=5)
        print(result)

        cursor.execute('SELECT name FROM test WHERE id < %s', ('3'))  # in cycle read with filter "id < 3"
        for row in cursor:
            print(row)

        cursor.execute('SELECT name FROM test WHERE id < %(id)s', {'id': 3})
        result = cursor.fetchone()  # read only first 1 row with filter "id < 3"
        print(result)

    cursor.close()
    pg_connection.close()

    # preferable way!
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        # copy table test to sample.csv
        with open(file=os.path.join('..', 'data', 'files', 'sample.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY test TO STDOUT WITH HEADER CSV', csv_file)

    pg_connection = psycopg2.connect(
        host='192.168.88.52',
        dbname='postgres',
        user='pguser',
        password='secret')

    cursor = pg_connection.cursor()
    # copy query from table test to sample2.csv
    query = 'select * from test where id < 3'
    with open(file=os.path.join('..', 'data', 'files', 'sample2.csv'), mode='w') as csv_file:
        cursor.copy_expert('COPY ({0}) TO STDOUT WITH HEADER CSV'.format(query), csv_file)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        # copy query from table test to sample3.csv
        with open(file=os.path.join('..', 'data', 'files', 'sample3.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY (select * from test where id < 3) TO STDOUT WITH HEADER CSV', csv_file)


def write_jdbc():
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        # previously in Postgres DB (by pg_creds) create empty table test2
        # copy data from csv file to table test2
        with open(file=os.path.join('..', 'data', 'files', 'sample.csv'), mode='r') as csv_file:
            cursor.copy_expert('COPY test2 FROM STDIN WITH HEADER CSV', csv_file)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        # directly insert data to test2
        data = [(4, 'd'), (5, 'e')]
        cursor.executemany("INSERT INTO test2 (id, name) VALUES (%s, %s)", data)


if __name__ == '__main__':
    read_jdbc()
    write_jdbc()
