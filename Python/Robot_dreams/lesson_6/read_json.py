import json
import os


def write_json():

    records = [
        {"model": "MX-100", "make": "Audi", "year": 2007},
        {"model": "DF-2", "make": "Opel", "year": 2030},
        {"model": "Corsa", "year": 2020},
        {"model": "ABF", "make": "Audi"},
          ]

    with open(file=os.path.join('.', 'data', 'cars.json'), mode='w') as json_file:
        json.dump(records, json_file)


def read_json():

    with open(file=os.path.join('.', 'data', 'cars.json'), mode='r') as json_file:
        record = json.load(json_file)
    print(record)


if __name__ == '__main__':
    write_json()
    read_json()