import os
from fastavro import writer, reader, parse_schema       # pip3.8 install fastavro


def write_avro():

    schema = {
        "namespace": "sample.avro",
        "type": "record",
        "name": "Cars",
        "fields": [
            {"name": "model", "type": "string"},
            {"name": "make", "type": ["string", "null"]},
            {"name": "year", "type": ["int", "null"]}
        ]
    }

    records = [
        {"model": "MX-100", "make": "Audi", "year": 2007},
        {"model": "DF-2", "make": "Opel", "year": 2030},
        {"model": "Corsa", "year": 2020},
        {"model": "ABF", "make": "Audi"},
          ]

    with open(file=os.path.join('.', 'data', 'cars.avro'), mode='wb') as avro_file:
        writer(avro_file, parse_schema(schema), records)


def read_avro():
    with open(file=os.path.join('.', 'data', 'cars.avro'), mode='rb') as avro_file:
        for record in reader(avro_file):
            print(record)


if __name__ == '__main__':
    write_avro()
    read_avro()