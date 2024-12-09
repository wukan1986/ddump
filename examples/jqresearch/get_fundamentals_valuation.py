import pandas as pd

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, jqr, DATA_ROOT_AKSHARE

"""
市值数据
每天更新   
"""


def main():
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT_AKSHARE / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    trading_day = trading_day['2024-12-01':end]

    func_name = f'get_fundamentals_valuation'

    path = DATA_ROOT / func_name
    d = Dump__date(jqr, path, 'date')
    for i, date in enumerate(trading_day):
        d.set_parameters(func_name, date=f'{date:%Y-%m-%d}')
        if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
            d.download(kw=['date'])
            d.save()


if __name__ == '__main__':
    main()
