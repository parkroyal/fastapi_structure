import logging
import os
import traceback
import functools
from datetime import date, datetime
from typing import Callable
from time import perf_counter, sleep

# from package.skype import skp_send

# from package.db_helper import config


traceback_printed = True
log_folder = "logs"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

logger = logging.getLogger(__name__)  # set logger name: if import
logger.setLevel(logging.DEBUG)  # setting level
formatter = logging.Formatter("%(filename)s - %(name)s - %(funcName)s - %(asctime)s - %(levelname)s :%(message)s")  # setting log format
file_name = os.path.join(log_folder, f"{date.today()}.log")  # write data to x.log

# file log
file_handler = logging.FileHandler(file_name)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
# streaming log
stream_formatter = logging.Formatter("%(levelname)s - %(funcName)s: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(stream_formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def log_exception(func):
    """
    Decorator provides simple exception handler: logs exception and throws it up
    :param func:
    :return: none or exception
    """

    @functools.wraps(func)
    def inner(*args, **kwargs):
        # global config
        global traceback_printed
        try:
            res = func(*args, **kwargs)
            # logger.info('end {}'.format(func.__name__))
        except Exception as e:
            if traceback_printed:
                logger.error("{} {}".format(func.__name__, e))
                # skp_send(
                #     user_id=config["skype"]["login"],
                #     password=config["skype"]["password"],
                #     config=config,
                #     content=f"{func.__code__.co_filename} - {func.__name__} Fail: {e}",
                #     who="Steven",
                #     file=file_name,
                # )
            else:
                logger.error("{} {}. \ntraceback: {}".format(func.__name__, e, traceback.format_exc()))
                traceback_printed = True
            raise e
        return res

    return inner


def benchmark(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        # 執行function
        res = func(*args, **kwargs)
        end_time = perf_counter()
        run_time = end_time - start_time
        logger.info(f"Function {func.__name__}{args} {kwargs} Took {run_time:.4f} seconds")
        return res

    return wrapper
