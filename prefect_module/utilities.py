from pymongo import MongoClient

from prefect import variables


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
