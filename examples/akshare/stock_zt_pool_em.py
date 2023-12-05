import time

import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

from ddump.api.dump import Dump__date
from examples.akshare.config import DATA_ROOT, ak


def save_func_stock_zt_pool_sub_new_em(df):
    """stock_zt_pool_sub_new_em中
    涨停价 上市日期
    需要修正，否则转parquet失败"""
    df['涨停价'] = pd.to_numeric(df['涨停价'], errors="coerce")
    df['上市日期'] = pd.to_numeric(df['上市日期'], errors="coerce").fillna(0).astype(int)
    return df


save_funcs = {
    'stock_zt_pool_sub_new_em': save_func_stock_zt_pool_sub_new_em,
}


@retry(wait=wait_fixed(15), stop=stop_after_attempt(5))
def main():
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # 加载交易日历
    trading_day = pd.read_parquet(DATA_ROOT / 'tool_trade_date_hist_sina' / f'calendar.parquet')
    trading_day = trading_day['trade_date']
    trading_day.index = pd.to_datetime(trading_day)
    # 过滤交易日
    # trading_day = trading_day['2020-01-01':'2020-02-01']
    # trading_day = trading_day['2012-02-27':'2012-02-29']
    trading_day = trading_day['2023-11-01':end]

    for func_name in [
        "stock_zt_pool_em",  # 涨停板行情-涨停股池
        "stock_zt_pool_previous_em",  # 涨停板行情-昨日涨停股池
        "stock_zt_pool_strong_em",  # 涨停板行情-强势股池
        "stock_zt_pool_sub_new_em",  # 涨停板行情-次新股池
        "stock_zt_pool_zbgc_em",  # 涨停板行情-炸板股池
        # "stock_zt_pool_dtgc_em",  # 涨停板行情-跌停股池
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(ak, path, 'date')
        for i, date in enumerate(trading_day):
            d.set_parameters(func_name, date=f'{date:%Y%m%d}')
            if not d.exists(file_timeout=3600 * 12, data_timeout=86400 * 1):
                d.download()
                d.save(save_empty=True, pre_save=save_funcs.get(func_name, None))
                # 这个地方按情况调整
                if i % 10 == 1:
                    time.sleep(10)


if __name__ == '__main__':
    main()
