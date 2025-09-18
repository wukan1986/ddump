import asyncio
from datetime import datetime

import pandas as pd
from ksrpc.client import RpcClient
from ksrpc.connections.websocket import WebSocketConnection

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT
from examples.jqresearch.config import URL, USERNAME, PASSWORD, JQR_MODULE

"""
除权除息数据下载

注意，分红时间的报告期不一定是按季出现
"""


async def download(jqr):
    for func_name in [
        "get_STK_XR_XD",
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(jqr, path, 'end_date')
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime('2023-01-15')
        start = pd.to_datetime('2023-01-01')
        end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=91)
        start = pd.to_datetime(datetime.today().date()) - pd.Timedelta(days=183)

        for dr in pd.date_range(start=start, end=end, freq='QE'):
            q = f'{dr:%Y-%m-%d}'
            d.set_parameters(func_name,
                             end_date=dr,
                             board_plan_pub_date=q)
            if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 90):
                # print(dr, q)
                await d.download(use_await=True, kw=['board_plan_pub_date'])
                d.save()


async def async_main():
    async with WebSocketConnection(URL, username=USERNAME, password=PASSWORD) as conn:
        jqr = RpcClient(JQR_MODULE, conn)
        await download(jqr)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
