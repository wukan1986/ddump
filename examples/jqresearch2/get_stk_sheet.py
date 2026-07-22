import asyncio
from datetime import datetime

import pandas as pd
from jupyter_data_fetch import LazyDownloader
from jupyter_data_fetch.download import JoinQuantDownloader
from jupyter_kernel_client import KernelClient

from ddump.api.dump import Dump__date
from examples.jqresearch2.config import SERVER_URL, HEADERS, DATA_ROOT, UID

"""
原始报表下载

资产负债表
利润表
现金流量表

如果数据下载是按报告期，这会导致要下载之前的数据基本上要全部重下一次，不现实
如果按发布期来算，就没有问题了
"""


async def download(jqr):
    for func_name in [
        "get_STK_BALANCE_SHEET",
        "get_STK_INCOME_STATEMENT",
        "get_STK_CASHFLOW_STATEMENT",
        "get_STK_FIN_FORCAST",
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(jqr, path, 'end_date')
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=91)
        start = pd.to_datetime(datetime.today().date()) - pd.Timedelta(days=183)
        # start = pd.to_datetime('2015-01-01')

        for dr in pd.date_range(start=start, end=end, freq='QE'):
            q = f'{dr:%Y-%m-%d}'
            d.set_parameters(func_name,
                             end_date=dr,
                             pub_date=q)
            if not d.exists(file_timeout=3600 * 1, data_timeout=86400 * 90):
                # print(dr, q)
                await d.download(use_await=False, kw=['pub_date'])
                d.save()


async def async_main():
    with KernelClient(server_url=SERVER_URL, token=None, headers=HEADERS) as kernel:
        downloader = JoinQuantDownloader(UID, HEADERS, delete=False)
        LazyDownloader.set_kernel(kernel)
        LazyDownloader.set_downloader(downloader)

        import examples.jqresearch2.jqresearch_query_client as jqr
        await download(jqr)

        downloader.cleanup()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
