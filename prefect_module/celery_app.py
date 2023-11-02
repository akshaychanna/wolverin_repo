from celery import Celery
from prefect import variables

app = Celery(
    broker=f"{variables.get('redis_local')}/1",
    backend=f"{variables.get('redis_local')}/2"
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
