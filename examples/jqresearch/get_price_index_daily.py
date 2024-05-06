from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""

# 下载日线数据
fields1 = ['open', 'close', 'high', 'low', 'volume', 'money', 'pre_close']
fq1 = None


def do_get_price(d, start_date, end_date, symbols, fields, fq):
    # 下载日线数据
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security=symbols, fq=fq, panel=False, fields=fields)
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        d.download(kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields'])
        d.save()


symbols = [
    "000001.XSHG",  # 上证指数
    "000016.XSHG",  # 上证50
    "000300.XSHG",  # 沪深300
    "000905.XSHG",  # 中证500
    "000852.XSHG",  # 中证1000
    "000688.XSHG",  # 科创50

    "399001.XSHE",  # 深证成指
    "399006.XSHE",  # 创业板指
    "399300.XSHE",  # 沪深300
    "399905.XSHE",  # 中证500
    "399852.XSHE",  # 中证1000
]

if __name__ == '__main__':
    types = 'index'

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')

    # 前半段，按周查，这样能快一些
    # end = pd.to_datetime('2024-05-06')  # 星期日
    # 下周，由date_range调到本周日
    end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=6)
    start = pd.to_datetime('2024-01-01')  # 星期一

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
        do_get_price(d1, start_date, end_date, symbols, fields1, fq1)
