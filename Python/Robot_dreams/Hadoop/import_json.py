# ! NOT run ot locally, only from ~/shared_folder/hadoop/ as >python import_json.py

import os
import json

from hdfs import InsecureClient


def app():
    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')        # inside VM local Hadoop
    client_hdfs.makedirs('/test')

    data = [{
                "Record number": 2,
                "ZipCode": 707,
                "ZipCodeType": "STANDARD",
                "City": "Paseo Costa Del Sur",
                "State": "PR"
            },
            {
                "Record number": 10,
                "ZipCode": 709,
                "ZipCodeType": "STANDARD",
                "City": "BDA San Luis",
                "State": "PR"
            }]

    # create files (2 different ways) in HDFS and write data
    with client_hdfs.write(os.path.join('/', 'test', 'multiline-zipcodes.json'),
                           encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as json_file_in_hdfs:
        json.dump(data, json_file_in_hdfs)

    client_hdfs.download(  # download file from HDFS test to local hdfs_data
        os.path.join('/', 'test', 'multiline-zipcodes.json'),
        os.path.join('/', 'home', 'user', 'hdfs_data', 'multiline-zipcodes.json'),
        overwrite=True)


if __name__ == '__main__':
    app()
