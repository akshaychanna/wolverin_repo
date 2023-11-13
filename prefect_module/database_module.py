from celery import Celery, Task
from celery.signals import worker_process_init, worker_process_shutdown
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from prefect import variables
from celery.contrib import rdb

app = Celery(
    "database_app",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1"
)

app.conf.update(
    result_backend=variables.get("mongo_prod"),
    result_extended=True,
    mongodb_backend_settings = {
        'database': 'celery_result_backend',
        'taskmeta_collection': 'celery_result_data',
    }
)

class DatabaseTask(Task):
    _db_client = None

    @classmethod
    def set_db_client(cls, db_client):
        cls._db_client = db_client
    
    @classmethod
    def close_db_client(cls):
        if cls._db_client:
            cls._db_client.close()

    @property
    def db_clinet(self):
        return self.__class__._db_client

def get_mongo_connections_count():
    client = MongoClient(variables.get("mongo_prod"))
    db = client["test_database"]
    server_status = db.command("serverStatus")
    num_connections = server_status['connections']['current']
    client.close()
    print(num_connections)


@worker_process_init.connect
def start_mongo_connection(**kwargs):
    get_mongo_connections_count()
    mongo_client = MongoClient(variables.get("mongo_prod"))
    DatabaseTask.set_db_client(mongo_client)
    get_mongo_connections_count()


@worker_process_shutdown.connect
def close_db_connection(**kwargs):
    get_mongo_connections_count()
    DatabaseTask.close_db_client()
    get_mongo_connections_count()
    print("closing database connection")


@app.task(base=DatabaseTask, name="utilities.database_worker")
def database_utility_worker(database_name, collection_name, action, query_parameters):
    try:
        db = database_utility_worker._db_client[database_name]
        collection = db[collection_name]

        if action == "insert_one":
            collection.insert_one(query_parameters.get("data"))
        elif action == "insert_many":
            collection.insert_many(query_parameters.get("data"))
        elif action == "update":
            collection.update_one(query_parameters.get("query"),
                                  {"$set": query_parameters.get("data")})
    except PyMongoError as e:
        print(f"An error occurred while performing the database operation: {e}")
    except KeyError as e:
        print(f"A key error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
