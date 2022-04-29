import yaml


def get_config():
    with open('../project/config.yaml', 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    return config


if __name__ == '__main__':
    config = get_config()

    table_list = "config.get('daily_etl').get('sources').get('postgres_dshopbu')"
    for table in eval(table_list):
        print(f'table - {table}')

        source_column_list = ''
        target_column_list = ''
        renameStr = ''
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
                        renameStr = renameStr + f".withColumnRenamed('{attribute}', '{types['name']}')"
                if j == 2:
                    csv_schema = csv_schema + f"{types['type']}Type(), "
                if j == 3:
                    csv_schema = csv_schema + f"{types['nullable']})"
                j = j + 1

            i = i + 1

        print(source_column_list)
        print(target_column_list)
        print(renameStr)
        csv_schema = csv_schema + '])'
        print(csv_schema)
