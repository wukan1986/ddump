import pandas as pd
from dateutil.relativedelta import relativedelta

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, jq, DATA_ROOT_AKSHARE

"""
指数权重
每月底更新   
"""

if __name__ == '__main__':
    # 上月底
    end = f"{pd.to_datetime('today') + relativedelta(months=-1, day=31):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT_AKSHARE / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    trading_day = trading_day['2024-06-01':end]
    trading_day = trading_day.resample('ME').last()

    func_name = f'get_index_weights'
    for index_id in [
        "000016.XSHG",  # 上证50
        "000300.XSHG",  # 沪深300
        "000905.XSHG",  # 中证500
        "000852.XSHG",  # 中证1000

        # "399300.XSHE",  # 沪深300
        # "399905.XSHE",  # 中证500
        # "399852.XSHE",  # 中证1000
    ]:
        path = DATA_ROOT / func_name / index_id
        d = Dump__date(jq, path, 'date')
        for i, date in enumerate(trading_day):
            d.set_parameters(func_name, index_id=index_id, date=f'{date:%Y-%m-%d}')
            if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 3):
                d.download(kw=['index_id', 'date'])
                d.save()
