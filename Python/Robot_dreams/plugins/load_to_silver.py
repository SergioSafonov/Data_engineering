import os
import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

from get_config import get_config

db_name = 'dshop'


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


def aisles_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    table_name = 'aisles'
    file_name = table_name + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    logging.info(f"Writing {table_name} from Bronze csv to Silver parquet")

    bronze_aisles_df = spark.read \
        .load(os.path.join(bronze_path, db_name, table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_aisles_df = bronze_aisles_df \
        .where(func.col('aisle').isNotNull()) \
        .withColumnRenamed('aisle', 'aisle_name') \
        .dropDuplicates()

    silver_aisles_df.write \
        .parquet(os.path.join(silver_path, db_name, table_name)
                 , mode='overwrite')

    logging.info(f"Successfully loaded {table_name} from Bronze csv to Silver parquet")


def clients_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    table_name = 'clients'
    file_name = table_name + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    logging.info(f"Writing {table_name} from Bronze csv to Silver parquet")

    bronze_clients_df = spark.read \
        .load(os.path.join(bronze_path, db_name, table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_clients_df = bronze_clients_df \
        .where(func.col('fullname').isNotNull()) \
        .withColumnRenamed('fullname', 'client_name') \
        .drop('location_area_id') \
        .dropDuplicates()

    silver_clients_df.write \
        .parquet(os.path.join(silver_path, db_name, table_name)
                 , mode='overwrite')

    logging.info(f"Successfully loaded {table_name} from Bronze csv to Silver parquet")


def departments_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    table_name = 'departments'
    file_name = table_name + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    logging.info(f"Writing {table_name} from Bronze csv to Silver parquet")

    bronze_departments_df = spark.read \
        .load(os.path.join(bronze_path, db_name, table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_departments_df = bronze_departments_df \
        .where(func.col('department').isNotNull()) \
        .withColumnRenamed('department', 'department_name') \
        .dropDuplicates()

    silver_departments_df.write \
        .parquet(os.path.join(silver_path, db_name, table_name)
                 , mode='overwrite')

    logging.info(f"Successfully loaded {table_name} from Bronze csv to Silver parquet")


def products_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    table_name = 'products'
    file_name = table_name + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    logging.info(f"Writing {table_name} from Bronze csv to Silver parquet")

    bronze_products_df = spark.read \
        .load(os.path.join(bronze_path, db_name, table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_products_df = bronze_products_df \
        .where(func.col('product_name').isNotNull()) \
        .dropDuplicates()

    silver_products_df.write \
        .parquet(os.path.join(silver_path, db_name, table_name)
                 , mode='overwrite')

    logging.info(f"Successfully loaded {table_name} from Bronze csv to Silver parquet")


def orders_silver_load(**kwargs):
    process_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    table_name = 'orders'
    file_name = table_name + '.csv'

    bronze_path, silver_path, spark = init_silver_load()

    logging.info(f"Writing {table_name} from Bronze csv to Silver parquet")

    bronze_orders_df = spark.read \
        .load(os.path.join(bronze_path, db_name, table_name, process_date, file_name),
              header="true",
              inferSchema="true",
              format="csv"
              )

    silver_orders_df = bronze_orders_df \
        .where(func.col('order_id').isNotNull()) \
        .where(func.col('quantity').isNotNull()) \
        .where(func.col('order_date').isNotNull()) \
        .withColumn('order_month', func.month('order_date')) \
        .dropDuplicates()

    silver_orders_df.write \
        .partitionBy('order_month') \
        .parquet(os.path.join(silver_path, db_name, table_name)
                 , mode='overwrite')

    logging.info(f"Successfully loaded {table_name} from Bronze csv to Silver parquet")


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


