"""

all kind of wrappers

"""

import time
from functools import wraps


def retry(retry_times, act_name, sleep_seconds):
    """

    :param retry_times:
    :param act_name:
    :param sleep_seconds:
    :return:
    """

    def retry_wrapper(func):
        """

        :param func:
        :return:
        """

        @wraps(func)
        def retry_func(*args, **kwargs):
            """
            :return:
            """
            for _ in range(retry_times):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
                    time.sleep(sleep_seconds)
            else:
                # send_dingding_and_raise_error(output_info)
                raise ValueError(act_name, '报错重试次数超过上限，程序退出。')

        return retry_func

    return retry_wrapper


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        r = func(*args, **kwargs)
        end = time.perf_counter()
        print('{}.{} : {}'.format(func.__module__, func.__name__, end - start))
        return r
    return wrapper
