import asyncio

import pandas as pd

from ddump.api.dump import Dump__start__end
from examples.jqdatasdk.config import DATA_ROOT, jq


class Wrapper:

    @staticmethod
    def get_trade_days(start_date, end_date):
        """由于输出格式比较特特殊, 只能特意封装一下"""
        arr = jq.get_trade_days(start_date=start_date, end_date=end_date)
        return pd.DataFrame(pd.to_datetime(arr), columns=['date'])


async def download():
    start = '2005-01-01'
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"

    path = DATA_ROOT / 'get_trade_days'
    d = Dump__start__end(Wrapper, path, 'start_date', 'end_date')
    for dr in pd.date_range(start=start, end=end, freq='YS'):
        start_date = f'{dr:%Y}-01-01'
        end_date = f'{dr:%Y}-12-31'
        d.set_parameters('get_trade_days', start_date=start_date, end_date=end_date)
        if not d.exists(file_timeout=86400 - 3600, data_timeout=86400 * 10):
            await d.download(use_await=False, kw=['start_date', 'end_date'])
            d.save()


async def async_main():
    await download()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
