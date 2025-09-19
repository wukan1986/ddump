import asyncio

from ddump.api.dump import Dump
from examples.tushare.config import DATA_ROOT, pro

"""
每次全查，L的频率可以高一些

"""


async def download(pro):
    path = DATA_ROOT / 'stock_basic'
    d = Dump(pro, path, file_names=['list_status'])
    for list_status in ('L',):  # 'L', 'D', 'P'
        d.set_parameters('stock_basic', list_status=list_status)
        if not d.exists(timeout=3600 * 12):
            await d.download(use_await=False, kw=['list_status'])
            d.save()
            await asyncio.sleep(60)


async def async_main():
    await download(pro)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
