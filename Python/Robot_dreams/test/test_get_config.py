import os
import yaml
# from config import Config

# config = Config(os.path.join('config.yaml'))
# config_set = config.get_config('daily_etl')

with open(os.path.join('../Config/config.yaml'), 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

config_set = config.get('daily_etl')
print(f"config_set: {config_set}")

sources = config_set.get('sources')
print(f"sources: {sources}")

table_list = sources['postgres_pagila']
print(f"table_list: {table_list}")

table_list = config.get('daily_etl').get('sources').get('postgres_pagila')
print(f"table_list 2: {table_list}")


