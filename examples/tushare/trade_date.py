import asyncio

import pandas as pd

from ddump.api.dump import Dump__date
from examples.tushare.config import DATA_ROOT, pro


async def download(pro):
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT / 'trade_cal' / f'SSE.parquet')
    trading_day = trading_day[trading_day['is_open'] == 1]['cal_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    # trading_day = trading_day['2020-01-01':'2020-02-01']
    # trading_day = trading_day['2020-01-01':'2021-12-31']
    trading_day = trading_day['2022-04-01':end]

    for func_name, m, n in [
        ('daily', 10, 1000),
        ('adj_factor', 10, 1000),
        ('suspend_d', 60, 50),  # 盘前就有，盘中有新增。每分钟只能访问50次
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(pro, path, 'trade_date')
        for i, trade_date in enumerate(trading_day):
            d.set_parameters(func_name, trade_date=trade_date)
            if not d.exists(file_timeout=3600 * 4, data_timeout=86400 * 2):
                await d.download(use_await=False, kw=['trade_date'])
                d.save()
                if i % n == n - 1:
                    await asyncio.sleep(m)


async def async_main():
    await download(pro)


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
