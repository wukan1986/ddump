import asyncio

from ddump.api.dump import Dump
from examples.akshare.config import DATA_ROOT, ak


async def download(ak):
    path = DATA_ROOT / 'tool_trade_date_hist_sina'
    d = Dump(ak, path, ['filename'])
    d.set_parameters('tool_trade_date_hist_sina', filename='calendar')
    if not d.exists(timeout=86400 * 1):
        await d.download(use_await=False, kw=[])
        d.save()
    print(d.load())


async def async_main():
    await download(ak)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
