from requests import Session
from requests.exceptions import ConnectionError
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
from requests.exceptions import Timeout
from requests.exceptions import TooManyRedirects
from fake_useragent import UserAgent as ua

from .logger_module import AsyncScraperLogger

logger = AsyncScraperLogger(__name__)


def request_handler(session: Session, url: str, method: str, **kwargs) -> tuple:
    """Universal class for managing requests with given parameters

    Args:
        session (Session): :class: requests.Session object from the trigger
        url (str): A proper URI for the respective web page/request
        method (str): GET/POST/PUT/DELETE/PATCH - case insensitive
        \*\*kwargs (dict): Other needed parameters for request

    Returns:
        tuple: (Response|None, Boolean)
        Response|None : Either web page :class: requests.Response object or None
        Boolean: Status of the requests True -> Success, False -> Failed
    """
    if not session:
        session = Session()
        session.headers.update({"user-agent": ua().random})
    scrape_job_id = kwargs.pop("scrape_job_id", "")
    logger.info(f"{scrape_job_id=} | requesting for {url=}")
    logger.debug(f"{scrape_job_id=} | Session cookies {session.cookies=}")
    try:
        response = session.request(method.upper(), url, **kwargs)
        logger.info(
            f"{scrape_job_id=} | status code for {url=} is <{response.status_code}>"
        )
        logger.debug(f"{scrape_job_id=} | TIme taken for response {response.elapsed}")
    except (ConnectionError, ConnectTimeout, ReadTimeout, Timeout, TooManyRedirects):
        logger.exception(f"{scrape_job_id=} | request failed")
    else:
        if response.status_code == 200:
            logger.info(f"{scrape_job_id=} | request is success, returing {response=}")
            return response, True
        else:
            # exception for website not available => SCR308
            logger.info(
                f"{scrape_job_id=} | Request to website failed, status code -> {response=}"  # noqa
            )
    return None, False


def prepare_session(url: str, method: str, **kwargs: dict) -> Session:
    """_summary_

    Args:
        url (str): _description_

    Returns:
        Session: _description_
    """
    web_session = Session()

    user_agent = ua().random
    web_session.headers.update({"user-agent": user_agent})  # automate with faker
    logger.info(f"Added user agent {user_agent=}")
    home_page_response, req_status = request_handler(
        session=web_session, url=url, method=method
    )

    return web_session
