from typing import Dict
from pickle import dumps, loads

from requests import Session
from prefect import task, flow, get_run_logger
from celery.contrib import rdb

from utilities import DatalakeConnect
from celery_app import app
from requests_module import prepare_session, request_handler

HOMEPAGE_URL = "https://mpbhulekh.gov.in/"


@app.task(name="mp_khasra_document_scraper")
def get_khasra_document(survey_meta:Dict, village_meta: Dict, scraper_session: Session):
    scraper_session = loads(scraper_session)
    khasra_url = f"https://mpbhulekh.gov.in/khasraCopyInNewFormate.do?distId={village_meta.get('district_code')}&tehsilId={village_meta.get('taluka_code')}&villageId={village_meta.get('village_code')}&ownerId=0&khasraId={survey_meta.get('survey_code')}&year=currentYear"
    print(f"khasra url => {khasra_url}")
    khasra_page, _ = request_handler(
        session=scraper_session, url=khasra_url, method="GET"
    )
    file_name = f"/Users/santhoshsolomon/Projects/mr_hulk/files/khasra_doc_{survey_meta.get('survey_code')}.html"
    with open(file_name, "w") as _f:
        _f.write(khasra_page.text)
    return file_name
    

@app.task(name="mp_khatuni_document_scraper")
def get_khatuni_document(survey_meta:Dict, village_meta: Dict, scraper_session: Session):
    scraper_session = loads(scraper_session)
    khatuni_url = f"https://mpbhulekh.gov.in/b1CopyInNewFormate.do?distId={village_meta.get('district_code')}&tehsilId={village_meta.get('taluka_code')}&villageId={village_meta.get('village_code')}&ownerId=0&khasraId={survey_meta.get('survey_code')}&year=currentYear"
    print(f"Khatuni url => {khatuni_url}")
    # rdb.set_trace()
    khatuni, _ = request_handler(
        session=scraper_session, url=khatuni_url, method="GET"
    )
    file_name = f"/Users/santhoshsolomon/Projects/mr_hulk/files/khatuni_doc_{survey_meta.get('survey_code')}.html"
    print(file_name)
    with open(file_name,"w") as _f:
        _f.write(khatuni.text)
    return file_name


@task(name="index_collector")
def get_indices(district_code:str):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="mp_survey_no_index"
    )
    distinct_villages = data_collection.distinct(
        "village_code", {"district_code": district_code}#, "survey_status":"P"}
    )
    distinct_villages = list(distinct_villages)
    datalake_connection.close_connection()
    return distinct_villages


@task(name="village_meta")
def get_village_meta(district_code:str, village_code: str):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="mp_survey_no_index"
    )
    village_meta = data_collection.find_one(
        {"district_code": district_code, "village_code": village_code},
        projection={"_id":0}
    )
    # datalake_connection.close_connection()
    return village_meta


@task(name="task_distributor")
def get_survey_data(village_code:str, village_meta: Dict):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="mp_survey_no_index"
    )
    village_survey_meta = data_collection.aggregate(
        [
            {"$match": {"village_code": village_code}},
            {
                "$project": {
                    "_id": 0,
                    "survey_code": "$survey_code",
                    "survey_number": "$survey_number",
                }
            },
        ]
    )
    village_survey_meta = list(village_survey_meta)
    datalake_connection.close_connection()
    return village_survey_meta


@task(name="session_generator")
def generate_web_session():
    logger = get_run_logger()
    scraper_session = prepare_session(HOMEPAGE_URL, method="GET")
    logger.info(f"{scraper_session.cookies}")
    return scraper_session


@task(name="distribute_khasra_tasks")
def get_khasra_documents(village_meta: Dict, survey_meta: Dict, scraper_session: Session, queue: str):
    logger = get_run_logger()
    # import pdb; pdb.set_trace()
    for survey_data in survey_meta:
        task_id = app.send_task(
            name="mp_khasra_document_scraper",
            args=[survey_data, village_meta, scraper_session],
            queue=f"{queue}_khasra_docs"
        )
        logger.info(f"task id for {survey_data.get('survey_number')} is {task_id}")

@task(name="distribute_khatuni_tasks")
def get_khatuni_documents(village_meta: Dict, survey_meta: Dict, scraper_session: Session, queue: str):
    logger = get_run_logger()
    # import pdb; pdb.set_trace()
    for survey_data in survey_meta:
        task_id = app.send_task(
            name="mp_khatuni_document_scraper",
            args=[survey_data, village_meta, scraper_session],
            queue=f"{queue}_khatuni_docs"
        )
        logger.info(f"task id for {survey_data.get('survey_number')} is {task_id}")


@flow(name="mp_lr_bulk")
def mp_land_record_bulk_scraper(district_code: str, queue_name: str):
    logger = get_run_logger()
    indices = get_indices(district_code=district_code)
    # for village in indices:
    #     logger.info(f"Village --> {village}")
    village = "486531" # for testing
    village_meta = get_village_meta(district_code=district_code, village_code=village)
    logger.info(f"Village meta data --> {village_meta}")
    survey_meta = get_survey_data(village_code=village, village_meta=village_meta)
    web_session = dumps(generate_web_session())
    logger.info("Task distribution starts")
    get_khasra_documents(village_meta, survey_meta, web_session, queue_name)
    get_khatuni_documents(village_meta, survey_meta, web_session, queue_name)


if __name__ == "__main__":
    mp_land_record_bulk_scraper("50", "test_queue")


    