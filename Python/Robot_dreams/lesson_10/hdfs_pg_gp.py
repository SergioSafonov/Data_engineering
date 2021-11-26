# ! NOT run ot locally, only from ~/shared_folder/hadoop/ as >python hdfs_pg_gp.py

import os
import psycopg2

from hdfs import InsecureClient     # library docs https://hdfscli.readthedocs.io/en/latest/index.html

postgres_creds = {
    'host': '192.168.88.138',
    'port': 5432,
    'database': 'postgres',
    'user': 'pguser',
    'password': 'secret'
}

greenplum_creds = {
    'host': '192.168.88.138',
    'port': '5433',
    'database': 'postgres',
    'user': 'gpuser',
    'password': 'secret'
}


def app():

    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')
    # client_hdfs = InsecureClient('http://192.168.88.138:50070/', user='user')

    # copy table from Postgres to HDFS
    with psycopg2.connect(**postgres_creds) as postgres_connection:
        cursor = postgres_connection.cursor()

        with client_hdfs.write(os.path.join('/', 'test', 'users.csv')) as csv_file:         # write to HDFS csv file
            client_hdfs.delete(os.path.join('/', 'test', 'users.csv'), recursive=False)
            cursor.copy_expert("COPY users(id,name) TO STDOUT WITH HEADER CSV", csv_file)   # Postgres table

    # copy data file from HDFS to Greenplum
    with psycopg2.connect(**greenplum_creds) as greenplum_connection:
        cursor = greenplum_connection.cursor()

        with client_hdfs.read(os.path.join('/', 'test', 'users.csv')) as csv_file:     # read csv file
            cursor.copy_expert("COPY users(id,name) FROM STDIN WITH HEADER CSV", csv_file)   # write to Greenplum table


if __name__ == '__main__':
    app()