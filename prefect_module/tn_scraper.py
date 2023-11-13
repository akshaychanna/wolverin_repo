from typing import Dict
from pymongo import MongoClient
from bson import ObjectId

from requests import Session,post
import requests
from prefect import task, flow, get_run_logger
from datetime import datetime 
from urllib.parse import urljoin

from utilities import DatalakeConnect
from celery_app import app
from requests_module import prepare_session, request_handler
from captcha_solver_module import internal_captcha_solver_with_bytes

HOMEPAGE_URL = "https://eservices.tn.gov.in/eservicesnew/land/"

@app.task(name="tn_survey_document_scraper")
def get_survey_document(survey_meta:Dict, village_meta: Dict, scraper_session: Session, captcha_result:Dict):
    logger = get_run_logger()
    captcha = captcha_result.get('result')[0]

    if captcha:
        data = {'task': 'chittaEng', 'role': 'null', 'viewOption': 'view', 'districtCode': village_meta['district_code'],
                    'talukCode': village_meta['taluka_code'], 'villageCode': village_meta['village_code'], 'viewOpt': 'sur', 'pattaNo': '', 
                    'surveyNo': str(survey_meta['survey_number']), 'subdivNo': survey_meta['subdivcode'], 'captcha': captcha}
        survey_page, _ = request_handler(session=scraper_session, url= urljoin(HOMEPAGE_URL,'chittaExtract_en.html?lan=en'), method="POST",data=data)
        logger.info('Response Received')

        file_name = f"/home/abhijeet/tn_lr_files/survey_doc_{survey_meta.get('survey_number')}_{survey_meta.get('subdivcode')}.html"
        print(survey_meta['_id'])
        if survey_page:
            if "This is Government Land." not in survey_page.text:
                error_msgs = ["Incorrect Value(enter the same as shown)" , "INVALID ACCESS TO THE SITE!" , "Enter Valid Survey Number or Subdvn Number." ] 
                if not any(term in survey_page.text for term in error_msgs):
                    with open(file_name, "w") as _f:
                        _f.write(survey_page.text)
                    logger.info(f'Html saved on -> {file_name}')
                    update_status(survey_meta['_id'], {"meta_data.scrape_status":True})
                    logger.info(f'Data found::reference mongo index id = {survey_meta["_id"]}')
                    return file_name
                else:
                    update_status(survey_meta['_id'], {"meta_data.error_data.scrape_error":True, "meta_data.error_data.error_details":"Incorrect Value(enter the same as shown) or INVALID ACCESS TO THE SITE! or Enter Valid Survey Number or Subdvn Number."})
                    logger.info("Error :: Incorrect Value(enter the same as shown) or INVALID ACCESS TO THE SITE! or Enter Valid Survey Number or Subdvn Number.")
            else:
                update_status(survey_meta['_id'], {"meta_data.scrape_status":True})
                logger.info(f'This is Government Land::reference mongo index id = {survey_meta["_id"]}')
        else:
            update_status(survey_meta['_id'], {"meta_data.error_data.scrape_error":True, "meta_data.error_data.error_details":"None type response" })
            logger.info(f'None type response::reference mongo index id = {survey_meta["_id"]}')
    else:
        logger.info(f'Internal Captcha error::reference mongo index id = {survey_meta["_id"]}')

@task(name="index_collector")
def update_status(unique_id, value):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="tamilnadu_bulk_index"
    )
    data_collection.update_one({ '_id': ObjectId(str(unique_id))}, {'$set': {**value, "scrape_time": datetime.now()}})
    datalake_connection.close_connection()
    return
    
@task(name="index_collector")
def get_indices(district_code:str):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="tamilnadu_bulk_index"
    )
    distinct_villages = data_collection.distinct(
        "village_code", {'district_code': district_code, "meta_data.scrape_status":False}
    )
    distinct_villages = list(distinct_villages)
    datalake_connection.close_connection()
    return distinct_villages


@task(name="village_meta")
def get_village_meta(district_code:str, village_code: str):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="tamilnadu_bulk_index"
    )
    village_meta = data_collection.find_one(
        {"district_code": district_code, "village_code": village_code},
        projection={"_id":0}
    )
    datalake_connection.close_connection()
    return village_meta


@task(name="task_distributor")
def get_survey_data(village_code:str, village_meta: Dict):
    logger = get_run_logger()
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="tamilnadu_bulk_index"
    )
    village_survey_meta = data_collection.aggregate(
        [
            {"$match": {"village_code": village_code, 'meta_data.scrape_status':False}},
            {
                "$project": {
                    "_id": "$_id",
                    "subdivcode": "$subdivcode",
                    "survey_number": "$survey_number",
                }
            },
        ]
    )
    village_survey_meta = list(village_survey_meta)
    datalake_connection.close_connection()
    return village_survey_meta


@task(name="distribute_khasra_tasks")
def get_khasra_documents(village_meta: Dict, survey_meta: Dict, scraper_session: Session, queue: str):
    logger = get_run_logger()
    for survey_data in survey_meta[:2]:
        captcha_response = scraper_session.get(urljoin(HOMEPAGE_URL,"simpleCaptcha.html"),verify=False)
        try:
            #captcha = captcha_integration(captcha_response.content,state_choice='def')
            captcha = internal_captcha_solver_with_bytes(captcha_image = image_bytes)
            print(captcha.get('result')[0])
        except Exception as e:
            logger.info('Internal captcha service failed!!')
        task_id = app.send_task(
            name="tn_survey_document_scraper",
            args=[survey_data, village_meta, scraper_session, captcha],
            queue=f"{queue}_khasra_docs"
        )
        logger.info(f"task id for survey no {survey_data.get('survey_number')} and sub-survey {survey_data.get('subdivcode')} is {task_id}")

@flow(name="mp_lr_bulk")
def tamilnadu_land_record_bulk_scraper(district_code: str, queue_name: str):
    logger = get_run_logger()
    indices = get_indices(district_code=district_code)
    # for village in indices:
    #     logger.info(f"Village --> {village}")
    village = "260" # for testing
    village_meta = get_village_meta(district_code=district_code, village_code=village)
    logger.info(f"Village meta data --> {village_meta}")
    survey_meta = get_survey_data(village_code=village, village_meta=village_meta)

    web_session = requests.Session()
    logger.info("Task distribution starts")
    get_khasra_documents(village_meta, survey_meta, web_session, queue_name)


if __name__ == "__main__":
    tamilnadu_land_record_bulk_scraper("17", "test_queue")


    