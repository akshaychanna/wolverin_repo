from time import perf_counter

from utilities.logger_module import AsyncScraperLogger
from utilities.requests_module import prepare_session

from request_module.madhya_pradesh_land_records import (
    solve_captcha,
    get_survey_meta_data,
    get_village_meta_data,
    collect_khasra_document,
    collect_khatuni_document,
    prepare_payload,
)

logger = AsyncScraperLogger("MP-SCRAPER")

HOMEPAGE_URL = "https://mpbhulekh.gov.in/"


def scraper_blue_print(district_code: str):
    scraper_session = prepare_session(HOMEPAGE_URL, method="GET")
    logger.info(f"{scraper_session.cookies}")
    captcha_result = solve_captcha(scraper_session)
    logger.info(f"{captcha_result=}")
    if captcha_result.get("status"):
        captcha_text = captcha_result.get("result")
        village_codes = get_village_meta_data(district_code=district_code)
        for village_code in village_codes:
            logger.info(f"{village_code=}")
            start = perf_counter()
            survey_codes, village_info = get_survey_meta_data("486530")
            for survey_code in survey_codes:
                logger.info(f"{survey_code=}")
                # import pdb; pdb.set_trace()
                payload = prepare_payload(village_info, survey_code, captcha_text)
                collect_khasra_document(payload, scraper_session)
                collect_khatuni_document(payload, scraper_session)
            end = perf_counter() - start
            print(f"Total time taken for a village -> {end}")
            exit()
    else:
        # retry captcha failure
        ...


scraper_blue_print("50")
