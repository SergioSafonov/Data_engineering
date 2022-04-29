import os
import logging

from requests.exceptions import HTTPError
from airflow.exceptions import AirflowException

import pyspark.sql.functions as func

from get_config import get_config, get_hadoop_path

config = get_config()
db_name = config.get('hadoop_path').get('postgres_db')

silver_path = get_hadoop_path('silver')


def dshopbu_gold_load(table, spark, gp_conn, **kwargs):
    order_date = kwargs["prev_ds"]  # yesterday

    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_properties = {'user': gp_conn.login, 'password': gp_conn.password}

    if table == 'client':
        client_gold_load(spark, gp_url, gp_properties, table)
    if table == 'product':
        product_gold_load(spark, gp_url, gp_properties, table)
    if table == 'store':
        store_gold_load(spark, gp_url, gp_properties, table)
    if table == 'date':
        date_gold_load(spark, gp_url, gp_properties, table)
    if table == 'order':
        order_gold_load(spark, gp_url, gp_properties, order_date, table)


def client_gold_load(spark, gp_url, gp_properties, gold_table):
    logging.info(f"Writing table {gold_table} from Silver to Greenplum data_mart!")

    try:
        client_df = spark.read.parquet(os.path.join(silver_path, db_name, gold_table))
        location_area_df = spark.read.parquet(os.path.join(silver_path, db_name, 'location_area'))

        client_gold_df = client_df \
            .join(location_area_df
                  , client_df.location_area_id == location_area_df.location_area_id
                  , 'left') \
            .select('client_id', 'client_name', 'location_area_name')

        client_gold_df.write \
            .jdbc(gp_url
                  , table="public.dim_client"
                  , properties=gp_properties
                  , mode="overwrite")

        logging.info(f"Successfully loaded {gold_table} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {gold_table} from Silver to Greenplum data_mart!")


def product_gold_load(spark, gp_url, gp_properties, gold_table):
    logging.info(f"Writing table {gold_table} from Silver to Greenplum data_mart!")

    try:
        product_df = spark.read.parquet(os.path.join(silver_path, db_name, gold_table))
        department_df = spark.read.parquet(os.path.join(silver_path, db_name, 'department'))
        aisle_df = spark.read.parquet(os.path.join(silver_path, db_name, 'aisle'))

        product_gold_df = product_df \
            .join(department_df
                  , product_df.department_id == department_df.department_id
                  , 'left') \
            .join(aisle_df
                  , product_df.aisle_id == aisle_df.aisle_id
                  , 'left') \
            .select('product_id', 'product_name', 'department_name', 'aisle_name')

        product_gold_df.write \
            .jdbc(gp_url
                  , table="public.dim_product"
                  , properties=gp_properties
                  , mode="overwrite")

        logging.info(f"Successfully loaded {gold_table} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {gold_table} from Silver to Greenplum data_mart!")


def store_gold_load(spark, gp_url, gp_properties, gold_table):
    logging.info(f"Writing table {gold_table} from Silver to Greenplum data_mart!")

    try:
        store_df = spark.read.parquet(os.path.join(silver_path, db_name, gold_table))
        location_area_df = spark.read.parquet(os.path.join(silver_path, db_name, 'location_area'))
        store_type_df = spark.read.parquet(os.path.join(silver_path, db_name, 'store_type'))

        store_gold_df = store_df \
            .join(location_area_df
                  , store_df.location_area_id == location_area_df.location_area_id
                  , 'left') \
            .join(store_type_df
                  , store_df.store_type_id == store_type_df.store_type_id
                  , 'left') \
            .select('store_id', 'store_type_name', 'location_area_name')

        store_gold_df.write \
            .jdbc(gp_url
                  , table="public.dim_store"
                  , properties=gp_properties
                  , mode="overwrite")

        logging.info(f"Successfully loaded {gold_table} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {gold_table} from Silver to Greenplum data_mart!")


def date_gold_load(spark, gp_url, gp_properties, gold_table):
    logging.info(f"Writing table {gold_table} from Silver to Greenplum data_mart!")

    try:
        date_df = spark.read.parquet(os.path.join(silver_path, db_name, gold_table))

        date_gold_df = date_df \
            .select('date', 'day', 'month', 'quarter', 'year', 'week_day', 'week')

        date_gold_df.write \
            .jdbc(gp_url
                  , table="public.dim_date"
                  , properties=gp_properties
                  , mode="overwrite")

        logging.info(f"Successfully loaded {gold_table} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {gold_table} from Silver to Greenplum data_mart!")


def order_gold_load(spark, gp_url, gp_properties, order_date, gold_table):
    logging.info(f"Writing table {gold_table} for {order_date} from Silver to Greenplum data_mart!")

    try:
        order_df = spark.read.parquet(os.path.join(silver_path, db_name, gold_table))

        order_gold_df = order_df \
            .where(order_df.order_date == order_date) \
            .select('order_id', 'order_month', 'order_date', 'store_id', 'client_id', 'product_id', 'quantity')

        order_gold_df.write \
            .option("reWriteBatchInserts", "true") \
            .jdbc(gp_url
                  , table="public.fact_order"
                  , properties=gp_properties
                  , mode="append")

        logging.info(f"Successfully loaded {gold_table} for {order_date} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {gold_table} for {order_date} from Silver to Greenplum data_mart!")


def out_of_stocks_config_gold_load(process_date, spark, gp_conn):
    out_of_stocks_gold_load(process_date, spark, gp_conn)


def out_of_stocks_current_gold_load(spark, gp_conn, **kwargs):
    process_date = kwargs["ds"]

    out_of_stocks_gold_load(process_date, spark, gp_conn)


def out_of_stocks_gold_load(process_date, spark, gp_conn):
    try:
        dir_name = config.get('rd_dreams_app').get('directory')

        gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
        gp_properties = {'user': gp_conn.login, 'password': gp_conn.password}

        logging.info(f"Writing {dir_name} data for {process_date} from Silver to Greenplum data_mart!")

        oos_silver_df = spark.read.parquet(os.path.join(silver_path, dir_name, dir_name))

        oos_gold_df = oos_silver_df \
            .where(oos_silver_df.process_date == process_date) \
            .select('product_id', 'process_date')

        oos_gold_df.write \
            .jdbc(gp_url
                  , table='public.fact_out_of_stock'
                  , properties=gp_properties
                  , mode='append')

        logging.info(f"Successfully loaded {dir_name} data for {process_date} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load {dir_name} data for {process_date} from Silver to Greenplum data_mart!")


def currency_gold_load(currency_base, currency_path, context_date, spark, gp_conn, **kwargs):
    currencies = config.get('currency_app').get('symbols')

    if context_date:
        process_date = kwargs[f"{context_date}"]
    else:
        process_date = kwargs["ds"]

    try:
        gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
        gp_properties = {'user': gp_conn.login, 'password': gp_conn.password}

        logging.info(f"Writing currencies for {process_date} from Silver to Greenplum data_mart!")

        i = 1
        for currency in currencies:
            rate_name = currency + '_' + currency_base

            currency_silver_df = spark.read.parquet(os.path.join(silver_path, currency_path, rate_name))

            currency_silver_df = currency_silver_df \
                .where(currency_silver_df.rate_date == process_date) \
                .dropDuplicates() \
                .select('currency_base', 'currency', 'rate_date', 'rate_month', 'rate')

            if i == 1:
                currency_gold_df = currency_silver_df
            else:
                currency_gold_df = currency_gold_df.union(currency_silver_df)

            i = i + 1

        currency_gold_df.write \
            .jdbc(gp_url
                  , table='public.fact_currency_rate'
                  , properties=gp_properties
                  , mode='append')

        logging.info(f"Successfully loaded currencies for {process_date} from Silver to Greenplum data_mart!")

    except HTTPError:
        raise AirflowException(f"Error load currencies for {process_date} from Silver to Greenplum data_mart!")
