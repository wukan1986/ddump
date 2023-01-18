from datetime import datetime

import pandas as pd

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""

# 下载日线数据
# 下载复权因子
fields1 = ['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused']
fields2 = ['factor']
fq1 = None
fq2 = 'post'


def do_get_price(d, start_date, end_date, universe, fields, fq):
    symbols = universe.query(f'start_date<=@end_date.date() and end_date>=@start_date.date()')

    # 下载日线数据
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security=symbols.index.tolist(), fq=fq, panel=False, fields=fields)
    if not d.exists(file_timeout=3600 * 12, data_timeout=86400 * 2):
        d.download()
        d.save(save_empty=True)


def do_get_extras(d, start_date, end_date, universe, info):
    symbols = universe.query(f'start_date<=@end_date.date() and end_date>=@start_date.date()')

    # 下载日线数据
    d.set_parameters('get_extras',
                     info=info,
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security_list=symbols.index.tolist(), df=True)
    if not d.exists(file_timeout=3600 * 12, data_timeout=86400 * 2):
        d.download()
        d.save(save_empty=True)


if __name__ == '__main__':
    end = pd.to_datetime(datetime.today().date())

    types = 'stock'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    path2 = DATA_ROOT / f'get_price_{types}_factor'
    path3 = DATA_ROOT / f'get_extras_{types}_is_st'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')
    d2 = Dump__start__end(jq, path2, 'start_date', 'end_date')
    d3 = Dump__start__end(jq, path3, 'start_date', 'end_date')

    # 前半段，按周查，这样能快一些
    end = pd.to_datetime('2023-01-15')  # 星期日
    # 下周，由date_range调到本周日
    end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=6)
    start = pd.to_datetime('2022-12-12')  # 星期一
    for dr in pd.date_range(start=start, end=end, freq='W'):
        start_date = dr - pd.Timedelta(days=6)
        end_date = dr

        do_get_price(d1, start_date, end_date, universe, fields1, fq1)
        do_get_price(d2, start_date, end_date, universe, fields2, fq2)
        do_get_extras(d3, start_date, end_date, universe, 'is_st')
