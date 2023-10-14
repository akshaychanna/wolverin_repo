from typing import Dict
from typing import Generator
from typing import List

from requests import Session

from utilities.captcha_solver_module import internal_captcha_solver_with_bytes
from utilities.database_module import DatalakeConnect
from utilities.logger_module import AsyncScraperLogger
from utilities.requests_module import request_handler

logger = AsyncScraperLogger("MP-REQUEST-MODULE")

CAPTCHA_URL = "https://mpbhulekh.gov.in/captcha.do"
SEARCH_URL = "https://mpbhulekh.gov.in/newKhasraFormateDetails.do"


def get_village_meta_data(district_code: str) -> List:
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="mp_survey_no_index"
    )
    distinct_villages = data_collection.distinct(
        "village_code", {"district_code": district_code}
    )
    # datalake_connection.close_connection()
    return distinct_villages


def get_survey_meta_data(village_code: str) -> Generator:
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="mp_survey_no_index"
    )
    # village_meta = data_collection.find_one({"village_code": village_code})
    village_meta = data_collection.find_one({"village_code": "486530"})
    logger.info("Village meta obtained")
    village_survey_meta = data_collection.aggregate(
        [
            {"$match": {"village_code": "486530"}},
            {
                "$project": {
                    "_id": 0,
                    "survey_code": "$survey_code",
                    "survey_number": "$survey_number",
                }
            },
        ]
    )
    # datalake_connection.close_connection()
    return village_survey_meta, village_meta


def prepare_payload(village_info: Dict, survey_code: str, captcha_text: str) -> Dict:
    key_mapping = {
        "villageId": "village_code",
        "tehsilId": "taluka_code",
        "distId": "district_code",
        "tehName": "taluka_name",
        "vilName": "village_name",
    }
    request_payload = {
        key: village_info.get(value) for key, value in key_mapping.items()
    }
    request_payload.update(
        {
            "captchavalue": captcha_text,
            "khasraId": survey_code.get("survey_code"),
            "$": "",
            "flag": "1",
        }
    )
    return request_payload


def collect_khasra_document(payload: Dict, scraper_session: Session) -> str:
    khasra_url = f"https://mpbhulekh.gov.in/khasraCopyInNewFormate.do?distId={payload.get('distId')}&tehsilId={payload.get('tehsilId')}&villageId={payload.get('villageId')}&ownerId=0&khasraId={payload.get('khasraId')}&year=currentYear"
    logger.info(f"khasra url => {khasra_url}")
    khasra_page, _ = request_handler(
        session=scraper_session, url=khasra_url, method="GET"
    )
    with open(
        f"/Users/santhoshsolomon/Projects/async-scraper/khasra_doc_{payload.get('khasraId')}.html",
        "w",
    ) as _f:
        _f.write(khasra_page.text)


def collect_khatuni_document(payload: Dict, scraper_session: Session) -> str:
    khatuni_url = f"https://mpbhulekh.gov.in/b1CopyInNewFormate.do?distId={payload['distId']}&tehsilId={payload['tehsilId']}&villageId={payload['villageId']}&khasraId={payload.get('khasraId')}&ownerId=0&khId={payload.get('khasraId')}&ownerId=0&year=currentYear"
    logger.info(f"Khatuni url => {khatuni_url}")
    khasra_page, _ = request_handler(
        session=scraper_session, url=khatuni_url, method="GET"
    )
    with open(
        f"/Users/santhoshsolomon/Projects/async-scraper/khatuni_doc_{payload.get('khasraId')}.html",
        "w",
    ) as _f:
        _f.write(khasra_page.text)


def solve_captcha(scraper_session: Session):
    captcha_text = None
    captcha_image_content, captcha_request_status = request_handler(
        url=CAPTCHA_URL, method="GET", session=scraper_session
    )
    if captcha_request_status:
        captcha_text = internal_captcha_solver_with_bytes(
            captcha_image_content.content, save_image=True
        )
    return captcha_text
