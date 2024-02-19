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


def get_paths(root):
    return [
        (rf'{root}\get_extras_stock_is_st', False),
        (rf'{root}\get_industry_stock', False),
        (rf'{root}\get_price_stock_factor', False),
        (rf'{root}\get_price_stock_daily', False),
        (rf'{root}\get_fundamentals_balance', True),
        (rf'{root}\get_fundamentals_cash_flow', True),
        (rf'{root}\get_fundamentals_income', True),
        (rf'{root}\get_fundamentals_indicator', True),
        (rf'{root}\get_fundamentals_valuation', True),
        (rf'{root}\get_STK_XR_XD', True),
        (rf'{root}\get_STK_BALANCE_SHEET', True),
        (rf'{root}\get_STK_CASHFLOW_STATEMENT', True),
        (rf'{root}\get_STK_INCOME_STATEMENT', True),
    ]


root1 = r'D:\data\jqresearch'
root2 = r'M:\data\jqresearch'
paths1 = get_paths(root1)
paths2 = get_paths(root2)
for (path1, _), (path2, _) in zip(paths1, paths2):
    logger.info('=' * 60, )
    path1 = pathlib.Path(path1)
    path2 = pathlib.Path(path2)
    files = path_groupby_date(path1, path2)
    merge_files_dict(files, ignore_index=_, delete_src=False)
    remove_sub_range(path2)
