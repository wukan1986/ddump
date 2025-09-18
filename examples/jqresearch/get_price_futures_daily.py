import asyncio

import pandas as pd
from ksrpc.client import RpcClient
from ksrpc.connections.websocket import WebSocketConnection

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, DATA_ROOT_AKSHARE
from examples.jqresearch.config import URL, USERNAME, PASSWORD, JQA_MODULE, JQR_MODULE

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""

# 下载日线数据
# 下载复权因子
fields1 = ['open', 'close', 'low', 'high', 'volume', 'money', 'high_limit', 'low_limit', 'avg', 'pre_close',
           'open_interest']
fq1 = None


async def do_get_price(d, start_date, end_date, symbols, fields, fq):
    # 下载日线数据
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security=symbols.index.tolist(), fq=fq, panel=False, fields=fields)
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        # print(start_date, end_date)
        await d.download(use_await=True, kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields'])
        d.save()


def post_download_get_dominant_futures(df, end_date):
    """下载数据时很可能数据多一天，所以这里需要进行截断

    注意：数据的存储是当天所使用的主力合约，而数据是昨天收盘后生成存在了第二天
    """
    df = df[:end_date]
    return df


async def do_get_dominant_futures(d, date, end_date, symbols):
    d.set_parameters('get_dominant_futures_all',
                     symbols=symbols,
                     date=f'{date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}')
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        # print(date, end_date)
        await d.download(use_await=True,
                         kw=['symbols', 'date', 'end_date'],
                         post_download=post_download_get_dominant_futures,
                         post_download_kwargs={'end_date': f'{end_date:%Y-%m-%d}'})
        d.save()


async def download(jqa, jqr):
    types = 'futures'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')
    universe['index'] = universe.index
    universe['product'] = universe['index'].str.extract(r'([A-Z]+)\d+')

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    d1 = Dump__start__end(jqa, path1, 'start_date', 'end_date')
    path2 = DATA_ROOT / f'get_dominant_futures'
    d2 = Dump__start__end(jqr, path2, 'date', 'end_date')

    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT_AKSHARE / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    start = f"{pd.to_datetime('today') - pd.Timedelta(days=32):%Y-%m-%d}"
    trading_day = trading_day[start:end]

    # 只要跨月了就划分成两部分，实现指定月份也能加载不出错
    start_list = []
    end_list = []
    for i, date in enumerate(trading_day):
        start_list.append(date)
        end_list.append(date)

    # 下载数据
    for start_date, end_date in zip(start_list, end_list):
        # print(start_date, end_date)
        symbols = universe.query(f'start_date<=@end_date and end_date>=@start_date')

        await do_get_price(d1, start_date, end_date, symbols, fields1, fq1)
        symbols_list = sorted(symbols['product'].unique())
        await do_get_dominant_futures(d2, start_date, end_date, symbols_list)


async def async_main():
    async with WebSocketConnection(URL, username=USERNAME, password=PASSWORD) as conn:
        jqa = RpcClient(JQA_MODULE, conn)
        jqr = RpcClient(JQR_MODULE, conn)
        await download(jqa, jqr)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
