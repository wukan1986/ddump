"""
下载历史数据后，很早以前的数据可以合并。一年合并一次即可
"""
import pathlib

from ddump.api.merge import path_groupby_date
from ddump.merge import merge_files_dict

paths = [
    r'D:\data\jqresearch\get_extras_stock_is_st',
    r'D:\data\jqresearch\get_industry_stock',
    r'D:\data\jqresearch\get_price_stock_factor',
    r'D:\data\jqresearch\get_price_stock_daily',
    r'D:\data\jqresearch\get_fundamentals_balance',
    r'D:\data\jqresearch\get_fundamentals_cash_flow',
    r'D:\data\jqresearch\get_fundamentals_income',
    r'D:\data\jqresearch\get_fundamentals_indicator',
    r'D:\data\jqresearch\get_fundamentals_valuation',
]
for path in paths:
    path = pathlib.Path(path)
    files = path_groupby_date(path, path)
    merge_files_dict(files, ignore_index=False, delete_src=True)

if False:
    # 测试用
    path1 = r'D:\data\jqresearch\get_fundamentals_balance'
    path1 = pathlib.Path(path1)
    path2 = r'D:\data\jqresearch\get_fundamentals_balance_2'
    path2 = pathlib.Path(path2)
    files = path_groupby_date(path1, path2)
    merge_files_dict(files, ignore_index=False, delete_src=False)
