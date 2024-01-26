"""
下载历史数据后，很早以前的数据可以合并。一年合并一次即可

历史数据如果还没有验证，应当选取其它方式验证多次后再合并
因为只要发现某天数据有问题，只要删了那天的文件，即可重新下载对应部分

TODO: 财务表过了一段时间后可能有问题，需要特别注意
"""
import pathlib

from loguru import logger

from ddump.api.merge import path_groupby_date
from ddump.merge import merge_files_dict, remove_sub_range

paths1 = [
    r'D:\data\jqresearch\get_extras_stock_is_st',
    r'D:\data\jqresearch\get_industry_stock',
    r'D:\data\jqresearch\get_price_stock_factor',
    r'D:\data\jqresearch\get_price_stock_daily',
    r'D:\data\jqresearch\get_fundamentals_balance',
    r'D:\data\jqresearch\get_fundamentals_cash_flow',
    r'D:\data\jqresearch\get_fundamentals_income',
    r'D:\data\jqresearch\get_fundamentals_indicator',
    r'D:\data\jqresearch\get_fundamentals_valuation',
    r'D:\data\jqresearch\get_stk_xr_xd',
]

paths2 = [
    r'M:\data\jqresearch\get_extras_stock_is_st',
    r'M:\data\jqresearch\get_industry_stock',
    r'M:\data\jqresearch\get_price_stock_factor',
    r'M:\data\jqresearch\get_price_stock_daily',
    r'M:\data\jqresearch\get_fundamentals_balance',
    r'M:\data\jqresearch\get_fundamentals_cash_flow',
    r'M:\data\jqresearch\get_fundamentals_income',
    r'M:\data\jqresearch\get_fundamentals_indicator',
    r'M:\data\jqresearch\get_fundamentals_valuation',
    r'M:\data\jqresearch\get_stk_xr_xd',
]
for path1, path2 in zip(paths1, paths2):
    logger.info('=' * 60, )
    path1 = pathlib.Path(path1)
    path2 = pathlib.Path(path2)
    files = path_groupby_date(path1, path2)
    merge_files_dict(files, ignore_index=False, delete_src=False)
    remove_sub_range(path2)
