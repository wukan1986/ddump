import pandas as pd

from examples.tushare_api.config import DATA_ROOT, pro
from ddump.api.dump import Dump__date

if __name__ == '__main__':
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT / 'trade_cal' / f'SSE.parquet')
    trading_day = trading_day[trading_day['is_open'] == 1]['cal_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    # trading_day = trading_day['2020-01-01':'2020-02-01']
    # trading_day = trading_day['2012-02-27':'2012-02-29']
    trading_day = trading_day['2022-01-01':end]

    for func_name in [
        # 'daily',
        # 'adj_factor',
        'suspend_d',  # 盘前就有，盘中有新增
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(pro, path, 'trade_date')
        for trade_date in trading_day:
            d.set_parameters(func_name, trade_date=trade_date)
            if not d.exists(file_timeout=3600 * 1, data_timeout=86400 * 3):
                d.download()
                d.save(save_empty=True)
        break
