from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from ddump.api.dump import Dump__start__end
from examples.jqresearch.config import DATA_ROOT, jq

"""
行情数据

按合约代码下载，从开始上市时开始下载。
历史部分可以通过此方法快速下载，有条件的应当

"""

# 下载日线数据
# 下载复权因子
fields1 = ['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused']
fields2 = ['factor']
fq1 = None
fq2 = 'post'


def do_get_price(d, start_date, end_date, symbols, fields, fq):
    # 下载日线数据
    d.set_parameters('get_price',
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security=symbols.index.tolist(), fq=fq, panel=False, fields=fields)
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        d.download(kw=['start_date', 'end_date', 'security', 'fq', 'panel', 'fields'])
        d.save()


def save_func_get_extras(df: pd.DataFrame):
    df = df.stack(dropna=True).reset_index()
    df.columns = ['time', 'code', 'is_st']
    return df[df['is_st']]


def do_get_extras(d, start_date, end_date, symbols, info):
    # 下载日线数据
    d.set_parameters('get_extras',
                     info=info,
                     start_date=f'{start_date:%Y-%m-%d}', end_date=f'{end_date:%Y-%m-%d}',
                     security_list=symbols.index.tolist(), df=True)
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 2):
        d.download(kw=['info', 'start_date', 'end_date', 'security_list', 'df'])
        d.save(pre_save=save_func_get_extras)


def dict_get(x, name, val):
    # 从复杂dataframe中提取信息
    try:
        return x.get(name, val)
    except:
        return val


def save_func_get_industry(df, date):
    dd = pd.DataFrame.from_dict(df, orient='index')
    # zjw栏目下会出现空值
    df1 = dd.map(lambda x: dict_get(x, 'industry_code', None))
    df2 = dd.map(lambda x: dict_get(x, 'industry_name', None))
    df1['is_code'] = True
    df2['is_code'] = False
    df = pd.concat([df1, df2], axis=0)
    df['date'] = date
    return df


def do_get_industry(d, start_date, end_date, symbols):
    # 下载日线数据
    d.set_parameters('get_industry',
                     start_date=f'{start_date:%Y-%m-%d}',
                     end_date=f'{end_date:%Y-%m-%d}',
                     date=f'{end_date:%Y-%m-%d}',
                     security=symbols.index.tolist())
    if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 4):
        d.download(kw=['security', 'date'])
        d.save(pre_save=save_func_get_industry, pre_save_kwargs={'date': end_date})


if __name__ == '__main__':
    types = 'stock'
    universe = pd.read_parquet(DATA_ROOT / 'get_all_securities' / f'{types}.parquet')

    path1 = DATA_ROOT / f'get_price_{types}_daily'
    path2 = DATA_ROOT / f'get_price_{types}_factor'
    path3 = DATA_ROOT / f'get_extras_{types}_is_st'
    path4 = DATA_ROOT / f'get_industry_{types}'
    d1 = Dump__start__end(jq, path1, 'start_date', 'end_date')
    d2 = Dump__start__end(jq, path2, 'start_date', 'end_date')
    d3 = Dump__start__end(jq, path3, 'start_date', 'end_date')
    d4 = Dump__start__end(jq, path4, 'start_date', 'end_date')

    # 前半段，按周查，这样能快一些
    end = pd.to_datetime('2023-01-15')  # 星期日
    # 下周，由date_range调到本周日
    end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=6)
    start = pd.to_datetime('2024-06-01')  # 星期一
    # start = pd.to_datetime('2014-12-29')  # 星期一

    # 只要跨月了就划分成两部分，实现指定月份也能加载不出错
    start_list = []
    end_list = []
    for dr in pd.date_range(start=start, end=end, freq='W'):
        start_date = dr - pd.Timedelta(days=6)
        end_date = dr
        if start_date.month == end_date.month:
            start_list.append(start_date)
            end_list.append(end_date)
        else:
            start_list.append(start_date)
            end_list.append(start_date + relativedelta(day=31))
            start_list.append(end_date + relativedelta(day=1))
            end_list.append(end_date)

    # 下载数据
    for start_date, end_date in zip(start_list, end_list):
        symbols = universe.query(f'start_date<=@end_date.date() and end_date>=@start_date.date()')

        do_get_price(d1, start_date, end_date, symbols, fields1, fq1)
        do_get_price(d2, start_date, end_date, symbols, fields2, fq2)
        do_get_extras(d3, start_date, end_date, symbols, 'is_st')
        do_get_industry(d4, start_date, end_date, symbols)
