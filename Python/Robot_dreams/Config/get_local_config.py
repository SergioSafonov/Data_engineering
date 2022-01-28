import os
import yaml


def get_config():
    with open(os.path.join('..', 'Config', 'config.yaml'), 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    return config


def get_pagila_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('sources').get('postgres_pagila')

    return table_list


def get_dshop_tables():
    config = get_config()

    table_list = config.get('daily_etl').get('sources').get('postgres_dshop')

    return table_list


def get_payload_dates():
    config = get_config()

    payload_dates = config.get('rd_dreams_app').get('payload_dates')

    return payload_dates
