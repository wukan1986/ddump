import asyncio

from ksrpc.client import RpcClient
from ksrpc.connections.websocket import WebSocketConnection

from ddump.api.dump import Dump
from examples.jqresearch.config import DATA_ROOT
from examples.jqresearch.config import JQA_MODULE, PASSWORD, USERNAME, URL


async def download(jqa):
    path = DATA_ROOT / 'get_all_securities'
    for types in ['stock', 'futures', 'index']:
        d = Dump(jqa, path, ['types'])  # ‘stock’, ‘fund’, ‘index’, ‘futures’, ‘etf’, ‘lof’, ‘fja’, ‘fjb’
        d.set_parameters('get_all_securities', types=types)
        if not d.exists(timeout=3600 * 1):
            await d.download(use_await=True, kw=['types'])
            d.save()


async def async_main():
    async with WebSocketConnection(URL, username=USERNAME, password=PASSWORD) as conn:
        jqa = RpcClient(JQA_MODULE, conn)
        await download(jqa)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
