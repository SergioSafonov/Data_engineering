import os
import yaml

from pyspark.sql import SparkSession

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook

from hdfs import InsecureClient


def get_config():
    with open(os.path.join('/', 'home', 'user', 'airflow', 'plugins', 'config.yaml'), 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    return config


def get_hadoop_path(layer):
    config = get_config()

    start_path = config.get('hadoop_path').get('start_dir')

    if layer == 'bronze':
        next_path = config.get('hadoop_path').get('bronze_path')
    if layer == 'silver':
        next_path = config.get('hadoop_path').get('silver_path')

    result_path = os.path.join('/', start_path, next_path)

    return result_path


def get_hdfs():
    # HDFS credentials definition (from Airflow connections)
    config = get_config()

    hdfs_name = config.get('connections').get('hdfs')
    hdfs_conn = HttpHook.get_connection(hdfs_name)

    client_hdfs = InsecureClient(f"{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)

    return client_hdfs


def get_spark():
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/Distrib/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('RD_datalake') \
        .getOrCreate()

    return spark


def get_postgres_conn(type_conn):
    config = get_config()

    if type_conn == 'source':
        conn_name = config.get('connections').get('postgres_dshop_bu')

    if type_conn == 'target':
        conn_name = config.get('connections').get('greenplum')

    pg_conn = PostgresHook.get_connection(conn_name)

    return pg_conn


def get_pagila_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('sources').get('postgres_pagila')

    return table_list


def get_dshop_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('sources').get('postgres_dshop')

    return table_list


def get_dshopbu_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('sources').get('postgres_dshopbu')

    return table_list


def get_silver_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('silver')

    return table_list


def get_gold_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('gold')

    return table_list


def get_table_types(table):
    config = get_config()

    table_list = "config.get('daily_etl').get('sources').get('postgres_dshopbu')"

    source_column_list = ''
    target_column_list = ''
    rename_str = ''
    csv_schema = 'StructType(['

    attribute_list = table_list + f".get('{table}')"
    attr_list = eval(attribute_list)

    i = 1
    for attribute in attr_list:
        if i > 1:
            source_column_list = source_column_list + ', '
            target_column_list = target_column_list + ', '
            csv_schema = csv_schema + ', '

        csv_schema = csv_schema + f'StructField("{attribute}", '

        types_list = attribute_list + f".get('{attribute}')"
        j = 1
        for types in eval(types_list):
            if j == 1:
                source_column_list = source_column_list + attribute
                target_column_list = target_column_list + f"'{types['name']}'"

                if attribute != types['name']:
                    rename_str = rename_str + f".withColumnRenamed('{attribute}', '{types['name']}')"
            if j == 2:
                csv_schema = csv_schema + f"{types['type']}Type(), "
            if j == 3:
                csv_schema = csv_schema + f"{types['nullable']})"
            j = j + 1

        i = i + 1

    csv_schema = csv_schema + '])'

    return source_column_list, target_column_list, rename_str, csv_schema


def get_payload_dates():
    config = get_config()

    payload_dates = config.get('rd_dreams_app').get('payload_dates')

    return payload_dates
