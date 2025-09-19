"""
行业板块变动很小，可以每周只查一次即可
"""
import asyncio

import pandas as pd

from ddump.api.dump import Dump__date
from examples.akshare.config import DATA_ROOT, ak


async def download(ak):
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # end = '2022-11-04'
    date = pd.to_datetime(end)

    for func_name in [
        "stock_board_industry_name_em",  # 东方财富-行业板块
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(ak, path, 'date')
        d.set_parameters(func_name, date=date)
        if not d.exists(file_timeout=3600 * 0.5, data_timeout=86400 * 2):
            await d.download(use_await=False, kw=[])
            d.save()


async def async_main():
    await download(ak)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
