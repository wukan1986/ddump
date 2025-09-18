import asyncio

import pandas as pd
from ksrpc.client import RpcClient
from ksrpc.connections.websocket import WebSocketConnection

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, DATA_ROOT_AKSHARE
from examples.jqresearch.config import URL, USERNAME, PASSWORD, JQR_MODULE

"""
市值数据
每天更新   
"""


async def download(jqr):
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT_AKSHARE / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    start = f"{pd.to_datetime('today') - pd.Timedelta(days=32):%Y-%m-%d}"
    trading_day = trading_day[start:end]

    func_name = f'get_fundamentals_valuation'

    path = DATA_ROOT / func_name
    d = Dump__date(jqr, path, 'date')
    for i, date in enumerate(trading_day):
        d.set_parameters(func_name, date=f'{date:%Y-%m-%d}')
        if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
            # print(date)
            await d.download(use_await=True, kw=['date'])
            d.save()


async def async_main():
    async with WebSocketConnection(URL, username=USERNAME, password=PASSWORD) as conn:
        jqr = RpcClient(JQR_MODULE, conn)
        await download(jqr)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
