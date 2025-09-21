import asyncio

import more_itertools
import pandas as pd
from ksrpc.client import RpcClient
from ksrpc.connections.websocket import WebSocketConnection

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, DATA_ROOT_AKSHARE
from examples.jqresearch.config import URL, USERNAME, PASSWORD, JQA_MODULE

"""

"""
fields1 = ['open', 'close', 'low', 'high', 'volume', 'money', 'avg', 'open_interest']
fq1 = None


async def do_get_price(d, start_date, end_date, symbols, fields, fq):
    # 下载分钟数据，由于数据量太大，需要分批下载
    symbols_list = symbols.index.tolist()
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d} 20:00:00', end_date=f'{end_date:%Y-%m-%d} 16:00:00',
                     security=symbols_list, fq=fq, panel=False, fields=fields, frequency='1m')
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 1):
        # print(start_date, end_date)
        sym_iter = more_itertools.batched(symbols_list, 150)
        for syms in sym_iter:
            d.set_parameters('get_price',
                             start_date=f'{start_date:%Y-%m-%d} 20:00:00', end_date=f'{end_date:%Y-%m-%d} 16:00:00',
                             security=syms, fq=fq, panel=False, fields=fields, frequency='1m')
            await d.download(use_await=True,
                             kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields', 'frequency'])
        d.save()


async def download(jqa):
    types = 'futures'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')

    path = DATA_ROOT / f'get_price_{types}_minute'
    d = Dump__start__end(jqa, path, 'start_date', 'end_date')

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
        symbols = universe.query(f'start_date<=@end_date and end_date>=@start_date')
        if start_date.weekday() == 0:
            # 周一
            start_date = start_date - pd.Timedelta(days=3)
        else:
            start_date = start_date - pd.Timedelta(days=1)
        # print(start_date, end_date)
        await do_get_price(d, start_date, end_date, symbols, fields1, fq1)


async def async_main():
    async with WebSocketConnection(URL, username=USERNAME, password=PASSWORD) as conn:
        jqa = RpcClient(JQA_MODULE, conn)
        await download(jqa)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
