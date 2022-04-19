import os
import logging
from datetime import date

from requests.exceptions import HTTPError
from airflow.exceptions import AirflowException

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

from get_config import get_config

db_name = 'dshop_bu'


def init_silver_load():
    bronze_path = os.path.join('/', 'datalake', 'bronze')
    silver_path = os.path.join('/', 'datalake', 'silver')

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/Distrib/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('homework_7') \
        .getOrCreate()

    return bronze_path, silver_path, spark


# dshop_bu
def dshopbu_silver_load(table, **kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')

    try:
        bronze_path, silver_path, spark = init_silver_load()

        if table == 'location_area':
            location_area_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'aisle':
            aisle_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'department':
            department_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'store_type':
            store_type_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'client':
            client_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'product':
            product_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'store':
            store_silver_load(bronze_path, silver_path, spark, process_date, table)

        if table == 'order':
            order_silver_load(bronze_path, silver_path, spark, process_date, table)

        logging.info(f"Successfully loaded {table} from Bronze csv to Silver parquet")

    except HTTPError:
        raise AirflowException(f"Error load {table} from Bronze csv to Silver parquet")


def location_area_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'location_areas'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_location_area_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_location_area_df = bronze_location_area_df \
        .where(func.col('area').isNotNull()) \
        .withColumnRenamed('area_id', 'location_area_id') \
        .withColumnRenamed('area', 'location_area_name') \
        .dropDuplicates()

    silver_location_area_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def aisle_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'aisles'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_aisle_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_aisle_df = bronze_aisle_df \
        .where(func.col('aisle').isNotNull()) \
        .withColumnRenamed('aisle', 'aisle_name') \
        .dropDuplicates()

    silver_aisle_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def department_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'departments'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_department_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_department_df = bronze_department_df \
        .where(func.col('department').isNotNull()) \
        .withColumnRenamed('department', 'department_name') \
        .dropDuplicates()

    silver_department_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def store_type_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'store_types'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_store_type_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_store_type_df = bronze_store_type_df \
        .where(func.col('type').isNotNull()) \
        .withColumnRenamed('type', 'store_type_name') \
        .dropDuplicates()

    silver_store_type_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def client_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'clients'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_client_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_client_df = bronze_client_df \
        .where(func.col('fullname').isNotNull()) \
        .withColumn('id', func.col('id').cast('int')) \
        .withColumnRenamed('id', 'client_id') \
        .withColumnRenamed('fullname', 'client_name') \
        .dropDuplicates()

    silver_client_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def product_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'products'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_product_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_product_df = bronze_product_df \
        .where(func.col('product_name').isNotNull()) \
        .dropDuplicates()

    silver_product_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def store_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'stores'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_store_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_store_df = bronze_store_df \
        .withColumn('store_id', func.col('store_id').cast('int')) \
        .withColumn('location_area_id', func.col('location_area_id').cast('int')) \
        .withColumn('store_type_id', func.col('store_type_id').cast('int')) \
        .dropDuplicates()

    silver_store_df.write \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


def order_silver_load(bronze_path, silver_path, spark, process_date, silver_table_name):
    bronze_table_name = 'orders'
    file_name = bronze_table_name + '.csv'

    logging.info(f"Writing {file_name} from Bronze to Silver parquet")

    bronze_order_df = spark.read \
        .load(os.path.join(bronze_path, db_name, bronze_table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_order_df = bronze_order_df \
        .where(func.col('order_id').isNotNull()) \
        .where(func.col('order_date').isNotNull()) \
        .where(func.col('quantity').isNotNull()) \
        .withColumn('order_month', func.month('order_date')) \
        .dropDuplicates()

    silver_order_df.write \
        .partitionBy('order_month') \
        .parquet(os.path.join(silver_path, db_name, silver_table_name)
                 , mode='overwrite')


# out_of_stocks
def out_of_stocks_config_silver_load(process_date):
    out_of_stocks_silver_load(process_date)


def out_of_stocks_current_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')

    out_of_stocks_silver_load(process_date)


def out_of_stocks_silver_load(process_date):
    config = get_config()
    config_data = config.get('rd_dreams_app')

    dir_name = config_data['directory']
    file_name = config_data['file_name']

    try:
        bronze_path, silver_path, spark = init_silver_load()

        logging.info(f"Writing {dir_name}.{file_name} from Bronze to Silver parquet")

        bronze_oos_df = spark.read \
            .load(os.path.join(bronze_path, dir_name, process_date, file_name),
                  header="true",
                  inferSchema="true",
                  format="json"
                  )

        silver_oos_df = bronze_oos_df \
            .withColumnRenamed('date', 'process_date') \
            .dropDuplicates()

        silver_oos_df.write \
            .partitionBy('process_date') \
            .parquet(os.path.join(silver_path, dir_name)
                     , mode='append')

        logging.info(f"Successfully loaded {dir_name}.{file_name} from Bronze to Silver parquet")

    except HTTPError:
        raise AirflowException(f"Error load {dir_name}.{file_name} from Bronze to Silver parquet")


def currency_silver_load(currency, base, save_path, context_date, **kwargs):
    rate_name = currency + '_' + base
    file_name = rate_name + '.json'

    if context_date:
        process_date = context_date
    else:
        process_date = kwargs["ds"].strftime('%Y-%m-%d')  # str(date.today())   # - timedelta(days=1))

    try:
        bronze_path, silver_path, spark = init_silver_load()

        logging.info(f"Writing currency {file_name} for {process_date} from Bronze to Silver parquet")

        bronze_currency_df = spark.read \
            .load(os.path.join(bronze_path, save_path, process_date, file_name),
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
            .withColumnRenamed('base', 'currency_base') \
            .withColumnRenamed('date', 'rate_date') \
            .drop('success') \
            .drop('timestamp') \
            .drop('rates') \
            .dropDuplicates()

        silver_currency_df.write \
            .partitionBy('rate_month') \
            .parquet(os.path.join(silver_path, save_path, rate_name)
                     , mode='append')

        logging.info(f"Successfully loaded {file_name} for {process_date} from Bronze to Silver parquet")

    except HTTPError:
        raise AirflowException(f"Error load {file_name} for {process_date} from Bronze to Silver parquet")
