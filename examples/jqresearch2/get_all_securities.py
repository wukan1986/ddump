import asyncio

from jupyter_data_fetch.codec import LazyKernel
from jupyter_data_fetch.download import JoinQuantDownloader
from jupyter_kernel_client import KernelClient

from ddump.api.dump import Dump
from examples.jqresearch2.config import HEADERS, SERVER_URL, DATA_ROOT, UID


async def download(jqa):
    path = DATA_ROOT / 'get_all_securities'
    for types in ['stock', 'futures', 'index']:
        d = Dump(jqa, path, ['types'])  # ‘stock’, ‘fund’, ‘index’, ‘futures’, ‘etf’, ‘lof’, ‘fja’, ‘fjb’
        d.set_parameters('get_all_securities', types=types)
        if not d.exists(timeout=3600 * 1):
            await d.download(use_await=False, kw=['types'])
            d.save()


async def async_main():
    with KernelClient(server_url=SERVER_URL, token=None, headers=HEADERS) as kernel:
        dl = JoinQuantDownloader(UID, HEADERS)
        LazyKernel.set_kernel(kernel)
        LazyKernel.set_downloader(dl)

        import jupyter_data_fetch.wraps.jqdatasdk as jqa
        await download(jqa)

        dl.cleanup()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
