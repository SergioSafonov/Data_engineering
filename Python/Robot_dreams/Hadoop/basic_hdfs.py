# ! NOT run ot locally, only from ~/shared_folder/hadoop/ as >python basic_hdfs.py

import os
import json

from hdfs import InsecureClient


def app():
    client_hdfs = InsecureClient('http://127.0.0.1:50070/', user='user')        # inside VM local Hadoop
    # client_hdfs = InsecureClient('http://192.168.88.138:50070/', user='user')

    client_hdfs.makedirs('/test')  # create HDFS directory

    ll = client_hdfs.list('/')  # list  HDFS directories
    print(ll)

    data = [{'name': 'Anne', 'salary': 10000}, {'name': 'Victor', 'salary': 15000}]

    # create files (2 different ways) in HDFS and write data
    with client_hdfs.write(os.path.join('/', 'test', 'salary.json'),
                           encoding='utf-8', overwrite=True, blocksize=1048576, replication=1) as json_file_in_hdfs:
        json.dump(data, json_file_in_hdfs)      # blocksize - Hadoop block size

    client_hdfs.write(os.path.join('/', 'test', 'salary.json'), data=json.dumps(data),
                      encoding='utf-8', overwrite=False, append=True)

    client_hdfs.download(  # download file from HDFS test to local hdfs_data
        os.path.join('/', 'test', 'salary.json'),
        os.path.join('/', 'home', 'user', 'hdfs_data', 'some_file.json'),
        overwrite=True)
    # client_hdfs.download(                    # download all data in dir
    #    os.path.join('/', 'test'),
    #    os.path.join('/', 'home', 'user', 'hdfs_data'),
    #    overwrite=True)

    client_hdfs.upload(  # upload to HDFS test from local hdfs_data
        os.path.join('/', 'test'),
        os.path.join('/', 'home', 'user', 'hdfs_data', 'some_file.json'),
        cleanup=True
    )

    # rename file in HDFS
    client_hdfs.rename(os.path.join('/', 'test', 'some_file.json'), os.path.join('/', 'test', 'local_file.json'))

    # delete file from HDFS
    client_hdfs.delete(os.path.join('/', 'test', 'local_file.json'), recursive=False)


if __name__ == '__main__':
    app()
