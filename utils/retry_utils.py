import time
from functools import wraps
from utils.logger import get_logger

logger = get_logger(__name__)


def retry(max_attempts=3, delay_seconds=2):

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            for attempt in range(1, max_attempts + 1):

                try:
                    return func(*args, **kwargs)

                except Exception as e:

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}"
                    )

                    if attempt == max_attempts:
                        logger.error("Max retries reached")
                        raise

                    time.sleep(delay_seconds)

        return wrapper

    return decorator