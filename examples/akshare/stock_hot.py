"""
此接口无法提供历史数据，都是从网站上爬取的最新值，每次爬都不一样
所以需要每天收盘后都爬，否则无法记录历史

i问财接入不好用，将使用别的接口来实现

人气股，只是短期比较活跃。
部分大盘股人气也很高
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
        "stock_hot_rank_em",  # 股票热度-东财
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(ak, path, 'date')
        d.set_parameters(func_name, date=date)
        if not d.exists(file_timeout=3600 * 0.5, data_timeout=86400 * 1):
            await d.download(use_await=False, kw=[])
            d.save()

    for func_name in [
        "stock_hot_follow_xq",  # 股票热度-雪球
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(ak, path, 'date')
        d.set_parameters(func_name, date=date, symbol='最热门')
        if not d.exists(file_timeout=3600 * 0.5, data_timeout=86400 * 2):
            await d.download(use_await=False, kw=['symbol'])
            d.save()


async def async_main():
    await download(ak)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
