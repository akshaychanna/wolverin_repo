from typing import Generator, List, Dict

from requests import Session

from utilities.database_module import DatalakeConnect
from utilities.logger_module import AsyncScraperLogger
from utilities.requests_module import prepare_session, request_handler
from utilities.captcha_solver_module import internal_captcha_solver_with_bytes


logger = AsyncScraperLogger("MP-REQUEST-MODULE")

HOMEPAGE_URL = "https://mpbhulekh.gov.in/"
CAPTCHA_URL = "https://mpbhulekh.gov.in/captcha.do"
SEARCH_URL = "https://mpbhulekh.gov.in/newKhasraFormateDetails.do"

def get_village_meta_data(district_code:str) -> List:
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(database="Land_Records", collection="mp_survey_no_index")
    distinct_villages = data_collection.distinct("village_code", {"district_code": district_code})
    datalake_connection.close_connection()
    return distinct_villages
    

def get_survey_meta_data(village_code:str) -> Generator:
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(database="Land_Records", collection="mp_survey_no_index")
    village_meta = data_collection.find_one({"village_code": village_code})
    village_survey_meta = data_collection.distinct("survey_code", {"village_code": village_code})
    datalake_connection.close_connection()
    return village_survey_meta, village_meta


def prepare_payload(village_info: Dict, survey_code: str, captcha_text: str) -> Dict:
    key_mapping = {
        'villageId': 'village_code',
        'tehsilId': 'taluka_code',
        'distId': 'district_code',
        'tehName': 'taluka_name',
        'vilName': 'village_name'
    }
    request_payload = {
        key: village_info.get(value)
        for key, value in key_mapping.items()
    }
    request_payload.update({'captchavalue': captcha_text, 'khasraId': survey_code, '$': '', 'flag': '1'})
    return request_payload


def collect_outer_page_response(payload: Dict, scraper_session: Session) -> str:
    search_response = request_handler(
        session=scraper_session,
        url=SEARCH_URL,
        method="POST",
        data=payload
    )
    import pdb; pdb.set_trace()


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


def scraper_blue_print(district_code: str):
    scraper_session = prepare_session(HOMEPAGE_URL, method="GET")
    logger.info(f"{scraper_session.cookies}")
    captcha_result = solve_captcha(scraper_session)
    logger.info(f"{captcha_result=}")
    if captcha_result.get("status"):
        captcha_text = captcha_result.get("result")
        village_codes = get_village_meta_data(district_code=district_code)
        for village_code in village_codes:
            survey_codes, village_info =  get_survey_meta_data(village_code)
            for survey_code in survey_codes:
                payload = prepare_payload(village_info, survey_code, captcha_text)
                collect_outer_page_response(payload, scraper_session)
    else:
        # retry captcha failure
        ...

scraper_blue_print("50")
