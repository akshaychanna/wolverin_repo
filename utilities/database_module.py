from pymongo import MongoClient

from .settings import MONGO_CONNECTION_STRING

class DatalakeConnect:
    def __init__(self, connection_limit: int = 10) -> None:
        self.connection_pool_size = connection_limit
        self.conneciton = self.database_connection()

    def database_connection(self):
        connection = MongoClient(MONGO_CONNECTION_STRING, maxPoolSize=self.connection_pool_size)
        return connection

    def connect_to_collection(self, database: str, collection: str):
        return self.conneciton[database][collection]

    def close_connection(self):
        self.conneciton.close()