import asyncio
import logging

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_random, before_sleep_log

from ddump.api.dump import Dump__start__end
from examples.tushare.config import DATA_ROOT, pro

"""
# namechange
start_date/end_date是根据ann_date来的，但早期记录没有ann_date，所以只能按个股查询

"""


@retry(wait=wait_random(10, 20), stop=stop_after_attempt(3), before_sleep=before_sleep_log(logger, logging.DEBUG))
async def download(pro):
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"

    for func_name, start in [
        ('new_share', '2008-01-01'),  # 2013年没有新股记录
        ('namechange', '2010-01-01'),
    ]:
        path = DATA_ROOT / func_name
        d = Dump__start__end(pro, path, 'start_date', 'end_date')
        for dr in pd.date_range(start=start, end=end, freq='YS'):
            start_date = f'{dr:%Y}0101'
            end_date = f'{dr:%Y}1231'
            d.set_parameters(func_name, start_date=start_date, end_date=end_date)
            if not d.exists(file_timeout=86400 * 1, data_timeout=86400 * 10):
                await d.download(use_await=False, kw=['start_date', 'end_date'])
                d.save()
                await asyncio.sleep(6)


async def async_main():
    await download(pro)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
