from datetime import datetime

import pandas as pd

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

get_price(symbols.index.tolist(),
          start_date='2022-05-01',
          end_date='2022-05-31',
          # , 'factor', 'open_interest'
          fields=['open', 'close', 'high', 'low', 'volume', 'money', 'avg', 'high_limit', 'low_limit', 'pre_close', 'paused'],
          fq=None, count=None, panel=False)
          
get_price(symbols.index.tolist(),
          start_date='2022-05-01',
          end_date='2022-05-31',
          fields=['factor'],
          fq='post', count=None, panel=False)

"""


def do_it(d1, d2, start_date, end_date, universe, func_name):
    fields1 = ['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close',
               'paused']
    fields2 = ['factor']
    fq1 = None
    fq2 = 'post'

    symbols = universe.query(f'start_date<=@end_date and end_date>=@start_date')

    # 下载日线数据
    d1.set_parameters(func_name,
                      start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                      security=symbols.index.tolist(), fq=fq1, panel=False, fields=fields1)
    if not d1.exists(file_timeout=86400 * 3, data_timeout=86400 * 10):
        d1.download()
        d1.save(save_empty=True)

    # 下载复权因子
    d2.set_parameters(func_name,
                      start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                      security=symbols.index.tolist(), fq=fq2, panel=False, fields=fields2)
    if not d2.exists(file_timeout=86400 * 3, data_timeout=86400 * 10):
        d2.download()
        d2.save(save_empty=True)


if __name__ == '__main__':
    func_name = 'get_price'
    end = pd.to_datetime(datetime.today().date())

    types = 'stock'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    path2 = DATA_ROOT / f'get_price_{types}_factor'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')
    d2 = Dump__start__end(jq, path2, 'start_date', 'end_date')

    if True:
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime('2022-10-16')
        start = pd.to_datetime('2022-05-23')
        for dr in pd.date_range(start=start, end=end, freq='W'):
            start_date = dr - pd.Timedelta(days=6)
            end_date = dr

            do_it(d1, d2, start_date, end_date, universe, func_name)

    if False:
        # 每日更新，按交易日查
        end = pd.to_datetime('2022-07-20')
        start = pd.to_datetime('2022-07-18')
        for dr in pd.date_range(start=start, end=end, freq='B'):
            start_date = dr
            end_date = dr

            do_it(d1, d2, start_date, end_date, universe, func_name)
