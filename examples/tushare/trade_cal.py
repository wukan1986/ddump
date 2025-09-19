import asyncio
import logging

from loguru import logger
from tenacity import retry, stop_after_attempt, wait_random, before_sleep_log

from ddump.api.dump import Dump
from examples.tushare.config import DATA_ROOT, pro


@retry(wait=wait_random(10, 20), stop=stop_after_attempt(3), before_sleep=before_sleep_log(logger, logging.DEBUG))
async def download(pro):
    path = DATA_ROOT / 'trade_cal'
    d = Dump(pro, path, ['exchange'])
    for exchange in ('SSE', 'SZSE'):
        d.set_parameters('trade_cal', exchange=exchange)
        if not d.exists(timeout=86400 * 15):
            await d.download(use_await=False, kw=['exchange'])
            d.save()
            await asyncio.sleep(6)


async def async_main():
    await download(pro)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
