from requests import Session

from utilities.requests_module import prepare_session, request_handler
from utilities.logger_module import AsyncScraperLogger
from utilities.captcha_solver_module import internal_captcha_solver_with_bytes

logger = AsyncScraperLogger("MP-REQUEST-MODULE")

HOMEPAGE_URL = "https://mpbhulekh.gov.in/"
CAPTCHA_URL = "https://mpbhulekh.gov.in/captcha.do"


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


def scraper_blue_print():
    scraper_session = prepare_session(HOMEPAGE_URL, method="GET")
    logger.info(f"{scraper_session.cookies}")
    captcha_text = solve_captcha(scraper_session)
    logger.info(f"{captcha_text=}")


scraper_blue_print()
