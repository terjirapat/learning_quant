import time
import logging
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def log_execution_time(func):
    """Decorator that logs the execution time of a function.
    
    Args:
        func: The function to be decorated.
        
    Returns:
        The wrapped function that logs its execution time.
        
    Example:
        @log_execution_time
        def process_data(df):
            return df.transform()
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Starting function '%s'", func.__name__)
        start_time = time.perf_counter()

        result = func(*args, **kwargs)

        end_time = time.perf_counter()
        elapsed = end_time - start_time

        logger.info(
            "Function '%s' completed in %.4f seconds",
            func.__name__,
            elapsed
        )

        return result

    return wrapper
