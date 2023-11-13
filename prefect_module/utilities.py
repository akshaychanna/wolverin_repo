from pymongo import MongoClient
from prefect import variables
import os
from boto3 import client



class DatalakeConnect:
    def __init__(self, connection_limit: int = 10) -> None:
        self.connection_pool_size = connection_limit
        self.conneciton = self.database_connection()

    def database_connection(self):
        connection = MongoClient(
            variables.get("mongo_prod"), maxPoolSize=self.connection_pool_size
        )
        return connection

    def connect_to_collection(self, database: str, collection: str):
        return self.conneciton[database][collection]

    def close_connection(self):
        self.conneciton.close()


class UPLOAD_FILE:
    def __init__(self, bucket='advarisk-public') -> None:
        self.s3_connection = self.s3_conn(bucket)

    def s3_conn(bucket):
        return client(
            service_name='s3',
            region_name=variables['STORAGE_REGION'],
            aws_access_key_id=variables['STORAGE_ACCESS_KEY'],
            aws_secret_access_key=variables['STORAGE_SECRET_KEY'],
            endpoint_url=variables['STORAGE_ENDPOINT']
        )
    
    def upload(filename, key=None, bucket='advarisk-public',file_response=False):
        if file_response:
            open(filename,'wb').write(file_response)
        if os.path.exists(filename):
            try:
                self.s3_connection.upload_file(
                    filename,
                    bucket,
                    key)
            except Exception as e:
                print(f'{e}')
            finally:
                os.remove(filename)
        else:
            print("Error: can't find file or read data")
        

