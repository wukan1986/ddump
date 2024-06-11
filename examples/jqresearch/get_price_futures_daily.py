from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq, jqr

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""

# 下载日线数据
# 下载复权因子
fields1 = ['open', 'close', 'low', 'high', 'volume', 'money', 'high_limit', 'low_limit', 'avg', 'pre_close', 'open_interest']
fq1 = None


def do_get_price(d, start_date, end_date, symbols, fields, fq):
    # 下载日线数据
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security=symbols.index.tolist(), fq=fq, panel=False, fields=fields)
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        d.download(kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields'])
        d.save()


def do_get_dominant_futures(d, date, end_date, symbols):
    d.set_parameters('get_dominant_futures_all',
                     symbols=symbols,
                     date=f'{date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}')
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        d.download(kw=['symbols', 'date', 'end_date'])
        d.save()


if __name__ == '__main__':
    types = 'futures'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')
    universe['index'] = universe.index
    universe['product'] = universe['index'].str.extract(r'([A-Z]+)\d+')

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')
    path2 = DATA_ROOT / f'get_dominant_futures'
    d2 = Dump__start__end(jqr, path2, 'date', 'end_date')

    # 前半段，按周查，这样能快一些
    end = pd.to_datetime('2024-06-09')  # 星期日
    # 下周，由date_range调到本周日
    end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=6)
    start = pd.to_datetime('2023-10-02')  # 星期一
    start = pd.to_datetime('2015-01-01')  # 星期一

    # 只要跨月了就划分成两部分，实现指定月份也能加载不出错
    start_list = []
    end_list = []
    for dr in pd.date_range(start=start, end=end, freq='W'):
        start_date = dr - pd.Timedelta(days=6)
        end_date = dr
        if start_date.month == end_date.month:
            start_list.append(start_date)
            end_list.append(end_date)
        else:
            start_list.append(start_date)
            end_list.append(start_date + relativedelta(day=31))
            start_list.append(end_date + relativedelta(day=1))
            end_list.append(end_date)

    # 下载数据
    for start_date, end_date in zip(start_list, end_list):
        print(start_date, end_date)
        symbols = universe.query(f'start_date<=@end_date.date() and end_date>=@start_date.date()')

        do_get_price(d1, start_date, end_date, symbols, fields1, fq1)
        symbols_list = sorted(symbols['product'].unique())
        do_get_dominant_futures(d2, start_date, end_date, symbols_list)
