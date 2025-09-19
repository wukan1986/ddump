import asyncio

from ddump.api.dump import Dump
from examples.jqdatasdk.config import DATA_ROOT, jq


async def download(jq):
    path = DATA_ROOT / 'get_all_securities'
    for types in ['stock', 'futures']:
        d = Dump(jq, path, ['types'])  # ‘stock’, ‘fund’, ‘index’, ‘futures’, ‘etf’, ‘lof’, ‘fja’, ‘fjb’
        d.set_parameters('get_all_securities', types=types)
        if not d.exists(timeout=86400 * 15):
            await d.download(use_await=False, kw=['types'])
            d.save()


async def async_main():
    await download(jq)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
