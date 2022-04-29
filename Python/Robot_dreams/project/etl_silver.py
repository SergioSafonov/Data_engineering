import os
import logging
import time

from requests.exceptions import HTTPError
from airflow.exceptions import AirflowException

import pyspark.sql.functions as func
from pyspark.sql.types import *

from get_config import get_config, get_hadoop_path, get_table_types

config = get_config()
db_name = config.get('hadoop_path').get('postgres_db')

bronze_path = get_hadoop_path('bronze')
silver_path = get_hadoop_path('silver')


# dshop_bu
def dshopbu_silver_load(table, spark, **kwargs):
    process_date = kwargs["ds"]  # today
    order_date = kwargs["prev_ds"]  # yesterday

    if table == 'location_area':
        location_area_silver_load(spark, process_date, table)
    if table == 'aisle':
        aisle_silver_load(spark, process_date, table)
    if table == 'department':
        department_silver_load(spark, process_date, table)
    if table == 'store_type':
        store_type_silver_load(spark, process_date, table)
    if table == 'client':
        client_silver_load(spark, process_date, table)
    if table == 'product':
        product_silver_load(spark, process_date, table)
    if table == 'store':
        store_silver_load(spark, process_date, table)
    if table == 'order':
        order_silver_load(spark, process_date, order_date, table)

    logging.info(f"Successfully loaded {table} from Bronze csv to Silver parquet")


def location_area_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'location_areas'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_location_area_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_location_area_str = "bronze_location_area_df.where(func.col('area').isNotNull())" \
                                   f"{rename_str}.dropDuplicates()" \
                                   f".select({target_column_list})"
        silver_location_area_df = eval(silver_location_area_str)

        silver_location_area_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def aisle_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'aisles'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_aisle_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_aisle_str = "bronze_aisle_df.where(func.col('aisle').isNotNull())" \
                           f"{rename_str}.dropDuplicates()" \
                           f".select({target_column_list})"
        silver_aisle_df = eval(silver_aisle_str)

        silver_aisle_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def department_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'departments'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_department_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_department_str = "bronze_department_df.where(func.col('department').isNotNull())" \
                                f"{rename_str}.dropDuplicates()" \
                                f".select({target_column_list})"
        silver_department_df = eval(silver_department_str)

        silver_department_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def store_type_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'store_types'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_store_type_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_store_type_str = "bronze_store_type_df.where(func.col('type').isNotNull())" \
                                f"{rename_str}.dropDuplicates()" \
                                f".select({target_column_list})"
        silver_store_type_df = eval(silver_store_type_str)

        silver_store_type_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def client_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'clients'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_client_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_client_str = "bronze_client_df.where(func.col('fullname').isNotNull())" \
                            f"{rename_str}.dropDuplicates()" \
                            f".select({target_column_list})"
        silver_client_df = eval(silver_client_str)

        silver_client_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def product_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'products'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_product_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_product_str = "bronze_product_df.where(func.col('product_name').isNotNull())" \
                             f"{rename_str}.dropDuplicates()" \
                             f".select({target_column_list})"
        silver_product_df = eval(silver_product_str)

        silver_product_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def store_silver_load(spark, process_date, silver_table_name):
    bronze_table_name = 'stores'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_store_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_store_str = f"bronze_store_df{rename_str}.dropDuplicates()" \
                           f".select({target_column_list})"
        silver_store_df = eval(silver_store_str)

        silver_store_df.write \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='overwrite')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")


def order_silver_load(spark, process_date, order_date, silver_table_name):
    start_time = time.time()

    bronze_table_name = 'orders'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    try:
        source_column_list, target_column_list, rename_str, csv_schema_str = get_table_types(bronze_table_name)
        csv_schema = eval(csv_schema_str)

        bronze_order_df = spark.read \
            .option("header", True) \
            .schema(csv_schema) \
            .csv(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name))

        silver_order_str = "bronze_order_df.where(bronze_order_df.order_id.isNotNull())" \
                           ".where(bronze_order_df.order_date.isNotNull())" \
                           ".where(bronze_order_df.quantity.isNotNull())" \
                           f".where(bronze_order_df.order_date == '{order_date}')" \
                           f"{rename_str}.withColumn('order_month', func.month('order_date'))" \
                           ".dropDuplicates()" \
                           f".select({target_column_list}, 'order_month')"
        silver_order_df = eval(silver_order_str)

        silver_order_df.write \
            .partitionBy('order_month') \
            .parquet(os.path.join(silver_path, db_name, silver_table_name)
                     , mode='append')

        end_time = time.time()
        logging.info(f'--- Silver orders load - {(end_time - start_time)} seconds ---')

    except HTTPError:
        raise AirflowException(f"Error load {silver_table_name} from Bronze csv to Silver parquet")

    # date
    logging.info(f"Writing date from Bronze table orders to Silver parquet")

    try:
        silver_date_df = silver_order_df \
            .select(silver_order_df.order_date.alias('date')) \
            .distinct()

        silver_date_str = "silver_date_df" \
                          f".where(silver_date_df.date == '{order_date}')" \
                          ".select(silver_date_df.date, " \
                          "func.dayofmonth('date').alias('day'), " \
                          "func.month('date').alias('month'), " \
                          "func.quarter('date').alias('quarter'), " \
                          "func.year('date').alias('year'), " \
                          "func.dayofweek('date').alias('week_day'), " \
                          "func.weekofyear('date').alias('week') )"
        silver_date_df = eval(silver_date_str)

        silver_date_df.write \
            .parquet(os.path.join(silver_path, db_name, 'date')
                     , mode='append')

    except HTTPError:
        raise AirflowException(f"Error load date from Bronze csv to Silver parquet")


# out_of_stocks
def out_of_stocks_config_silver_load(process_date, spark):
    out_of_stocks_silver_load(process_date, spark)


def out_of_stocks_current_silver_load(spark, **kwargs):
    process_date = kwargs["ds"]

    out_of_stocks_silver_load(process_date, spark)


def out_of_stocks_silver_load(process_date, spark):
    config_data = config.get('rd_dreams_app')

    dir_name = config_data['directory']
    file_name = config_data['file_name']

    try:
        logging.info(f"Writing {dir_name}.{file_name} for {process_date} from Bronze to Silver parquet")

        bronze_oos_df = spark.read \
            .load(os.path.join(bronze_path, dir_name, process_date, file_name),
                  header="true",
                  inferSchema="true",
                  format="json"
                  )

        silver_oos_df = bronze_oos_df \
            .withColumn('date', func.col('date').cast('date')) \
            .withColumnRenamed('date', 'process_date') \
            .dropDuplicates()

        silver_oos_df = silver_oos_df \
            .where(silver_oos_df.process_date == process_date) \
            .select('product_id', 'process_date')

        silver_oos_df.write \
            .partitionBy('process_date') \
            .parquet(os.path.join(silver_path, dir_name, dir_name)
                     , mode='append')

        logging.info(f"Successfully loaded {dir_name}.{file_name} for {process_date} from Bronze to Silver parquet")

    except HTTPError:
        raise AirflowException(f"Error load {dir_name}.{file_name} for {process_date} from Bronze to Silver parquet")


def currency_silver_load(currency, currency_base, currency_path, context_date, spark, **kwargs):
    rate_name = currency + '_' + currency_base
    file_name = rate_name + '.json'

    if context_date:
        process_date = kwargs[f"{context_date}"]  # .strftime('%Y-%m-%d')
    else:
        process_date = kwargs["ds"]  # str(date.today())   # - timedelta(days=1))

    try:
        logging.info(f"Writing currency {file_name} for {process_date} from Bronze to Silver parquet")

        bronze_currency_df = spark.read \
            .load(os.path.join(bronze_path, currency_path, process_date, file_name),
                  header="true",
                  inferSchema="true",
                  format="json",
                  multiline="true"
                  )

        if currency == 'RUB':
            silver_currency_df = bronze_currency_df \
                .withColumn('rates.RUB', func.col('rates.RUB').cast('double')) \
                .withColumnRenamed('rates.RUB', 'rate')

        if currency == 'UAH':
            silver_currency_df = bronze_currency_df \
                .withColumn('rates.UAH', func.col('rates.UAH').cast('double')) \
                .withColumnRenamed('rates.UAH', 'rate')

        if currency == 'GBP':
            silver_currency_df = bronze_currency_df \
                .withColumn('rates.GBP', func.col('rates.GBP').cast('double')) \
                .withColumnRenamed('rates.GBP', 'rate')

        if currency == 'USD':
            silver_currency_df = bronze_currency_df \
                .withColumn('rates.USD', func.col('rates.USD').cast('double')) \
                .withColumnRenamed('rates.USD', 'rate')

        if currency == 'PLN':
            silver_currency_df = bronze_currency_df \
                .withColumn('rates.PLN', func.col('rates.PLN').cast('double')) \
                .withColumnRenamed('rates.PLN', 'rate')

        silver_currency_df = silver_currency_df \
            .withColumn('currency', func.lit(currency)) \
            .withColumn('rate_month', func.month('date')) \
            .withColumn('date', func.col('date').cast('date')) \
            .withColumnRenamed('base', 'currency_base') \
            .withColumnRenamed('date', 'rate_date') \
            .drop('success') \
            .drop('timestamp') \
            .drop('rates')

        silver_currency_df = silver_currency_df \
            .where(silver_currency_df.rate_date == process_date) \
            .dropDuplicates() \
            .select('currency_base', 'currency', 'rate_date', 'rate_month', 'rate')

        silver_currency_df.write \
            .partitionBy('rate_month') \
            .parquet(os.path.join(silver_path, currency_path, rate_name)
                     , mode='append')

        logging.info(f"Successfully loaded {file_name} for {process_date} from Bronze to Silver parquet")

    except HTTPError:
        raise AirflowException(f"Error load {file_name} for {process_date} from Bronze to Silver parquet")
