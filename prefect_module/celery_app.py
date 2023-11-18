from celery import Celery
from prefect import variables

app = Celery(
    broker=f"{variables.get('redis_prod')}/5",
    backend=f"{variables.get('redis_prod')}/6"
)

app.conf.update(
    result_backend=variables.get("mongo_prod"),
    result_extended=True,
    mongodb_backend_settings = {
        'database': 'celery_result_backend',
        'taskmeta_collection': 'celery_result_data',
    }
)

app.autodiscover_tasks(packages=["mp_scraper.*"], force=True)

# @signals.worker_process_init.connect
