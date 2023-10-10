from .logger_module import AsyncScraperLogger
from .requests_module import request_handler
from .settings import CAPTCHA_SOLVER_API_KEY
from .settings import CAPTCHA_SOLVER_URL
from .settings import ENV

logger = AsyncScraperLogger(__name__)


def internal_captcha_solver_with_bytes(
    captcha_image: bytes, model_choice: int = 0, save_image=False
) -> dict:
    """_summary_

    Args:
        captcha_image (bytes): _description_
        model_choice (int, optional): _description_. Defaults to 0.

    Returns:
        dict: _description_
    """

    if ENV == "DEV":
        save_image = True
    response = {"result": None, "status": False}

    files = {"file": captcha_image}
    headers = {"X-API-Key": CAPTCHA_SOLVER_API_KEY}
    if save_image:
        # save image with unique name
        with open("test.png", "wb") as _f:
            _f.write(captcha_image)
    solver_response, req_status = request_handler(
        session=None,
        url=CAPTCHA_SOLVER_URL,
        method="POST",
        files=files,
        headers=headers,
    )
    if req_status:
        try:
            response.update(
                {"result": solver_response.json()[model_choice] or None, "status": True}
            )
        except IndexError:
            logger.error(f"selected model is not available! Result => {response}")
    return response
