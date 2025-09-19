import asyncio

import pandas as pd

from ddump.api.common import get_last_file
from ddump.api.dump import Dump__start__end
from ddump.common import FILE_SUFFIX
from examples.jqdatasdk.config import DATA_ROOT, jq

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""
from datetime import datetime


async def download(jq):
    func_name = 'get_price'
    today = pd.to_datetime(datetime.today().date())
    today = pd.to_datetime('2020-01-01')
    begin = pd.to_datetime('2005-01-01')

    for types in ['stock']:
        if types == 'stock':
            fields = ['open', 'close', 'low', 'high', 'volume', 'money',
                      'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
        if types == 'futures':
            fields = ['open', 'close', 'low', 'high', 'volume', 'money',
                      'factor', 'high_limit', 'low_limit', 'avg', 'open_interest']

        universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')
        for symbol, row in universe.iterrows():
            path = DATA_ROOT / f'get_price_{types}' / f'{symbol}'

            try:
                # 有空白文件时，读文件夹可能出问题，所以还是取最后一个文件
                df = pd.read_parquet(get_last_file(path, FILE_SUFFIX))
                start = df.index[-1] + pd.Timedelta(days=1)
            except (FileNotFoundError, IndexError, TypeError) as e:
                start = row['start_date']
            start = max(begin, start)
            end = min(today, row['end_date'])

            d = Dump__start__end(jq, path, 'start_date', 'end_date')
            # 这里应当改成一次性下载完一支，而不是分年下载一支
            for dr in pd.date_range(start=start, end=end, freq='Y'):
                start_date = max(pd.to_datetime(f'{dr:%Y}0101'), start)
                end_date = min(pd.to_datetime(f'{dr:%Y}1231'), end)
                d.set_parameters(func_name,
                                 start_date=start_date, end_date=end_date,
                                 security=symbol, fq=None, panel=False, fields=fields)
                if not d.exists(file_timeout=86400 * 1, data_timeout=86400 * 10):
                    await d.download(use_await=False,
                                     kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields'])
                    d.save()
            # 测试用，在第一支股票后就跳出
            # break


async def async_main():
    await download(jq)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
