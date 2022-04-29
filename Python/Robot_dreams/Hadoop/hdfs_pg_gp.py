# ! NOT run ot locally, only from ~/shared_folder/hadoop/ as >python hdfs_pg_gp.py

import os
import psycopg2

from hdfs import InsecureClient  # library docs https://hdfscli.readthedocs.io/en/latest/index.html

postgres_dshop_creds = {
    'host': '192.168.0.16',
    'port': 5432,
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}
postgres_pagila_creds = {
    'host': '192.168.0.16',
    'port': 5432,
    'database': 'pagila',
    'user': 'pguser',
    'password': 'secret'
}
greenplum_creds = {
    'host': '192.168.0.16',
    'port': '5433',
    'database': 'gp',
    'user': 'gpuser',
    'password': 'secret'
}


def app():
    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')
    # client_hdfs = InsecureClient('http://192.168.88.138:50070/', user='user')

    # copy table from Postgres to HDFS
    with psycopg2.connect(**postgres_dshop_creds) as postgres_connection:
        cursor = postgres_connection.cursor()

        with client_hdfs.write(os.path.join('/', 'test', 'clients.csv')) as csv_file:  # write to HDFS csv file
            client_hdfs.delete(os.path.join('/', 'test', 'clients.csv'), recursive=False)
            # Postgres table
            cursor.copy_expert("COPY public.clients(client_id,fullname) TO STDOUT WITH HEADER CSV", csv_file)

    # copy table from Postgres to HDFS
    with psycopg2.connect(**postgres_pagila_creds) as postgres_connection:
        cursor = postgres_connection.cursor()

        with client_hdfs.write(os.path.join('/', 'test', 'payment.csv')) as csv_file:  # write to HDFS csv file
            client_hdfs.delete(os.path.join('/', 'test', 'payment.csv'), recursive=False)
            # Postgres table
            cursor.copy_expert("COPY (SELECT payment_id,customer_id,staff_id,rental_id,amount,payment_date FROM "
                               "public.payment) TO STDOUT WITH HEADER CSV", csv_file)
        # WHERE payment_date BETWEEN '2020-01-26 00:00:00' AND '2020-01-27 00:00:00'

    # copy data file from HDFS to Greenplum
    with psycopg2.connect(**greenplum_creds) as greenplum_connection:
        cursor = greenplum_connection.cursor()

        with client_hdfs.read(os.path.join('/', 'test', 'clients.csv')) as csv_file:  # read csv file
            # write to Greenplum table
            cursor.copy_expert("COPY public.clients(client_id,fullname) FROM STDIN WITH HEADER CSV", csv_file)

        with client_hdfs.read(os.path.join('/', 'test', 'payment.csv')) as csv_file:  # read csv file
            # write to Greenplum table
            cursor.copy_expert("COPY public.payment(payment_id,customer_id,staff_id,rental_id,amount,payment_date) "
                               "FROM STDIN WITH HEADER CSV", csv_file)


if __name__ == '__main__':
    app()
