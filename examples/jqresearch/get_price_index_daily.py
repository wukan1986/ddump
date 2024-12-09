import pandas as pd

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq, DATA_ROOT_AKSHARE

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


def main():
    types = 'index'

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')

    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT_AKSHARE / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    # end = f"2024-11-01"
    trading_day = trading_day['2024-12-01':end]

    # 只要跨月了就划分成两部分，实现指定月份也能加载不出错
    start_list = []
    end_list = []
    for i, date in enumerate(trading_day):
        start_list.append(date)
        end_list.append(date)

    # 下载数据
    for start_date, end_date in zip(start_list, end_list):
        # symbols = universe.query(f'start_date<=@end_date and end_date>=@start_date')
        do_get_price(d1, start_date, end_date, symbols, fields1, fq1)


if __name__ == '__main__':
    main()
