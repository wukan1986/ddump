"""
由于已经加了tmp文件名，其他下载脚本就能重新下载此文件。
等到下载后，两个文件共存，此代码可删除tmp文件

"""
import pandas as pd
from loguru import logger

from ddump.api.common import files_to_dataframe
from ddump.common import FILE_SUFFIX, TEMP_SUFFIX


def get_paths(root):
    return [
        (rf'{root}\get_extras_stock_is_st', False),
        (rf'{root}\get_industry_stock', False),
        (rf'{root}\get_price_stock_factor', False),
        (rf'{root}\get_price_stock_daily', False),

        # ignore_index=True 表示合并时丢弃索引，因为索引不含有效信息
        (rf'{root}\get_fundamentals_balance', True),
        (rf'{root}\get_fundamentals_cash_flow', True),
        (rf'{root}\get_fundamentals_income', True),
        (rf'{root}\get_fundamentals_indicator', True),
        (rf'{root}\get_fundamentals_valuation', True),
        (rf'{root}\get_STK_XR_XD', True),
        (rf'{root}\get_STK_BALANCE_SHEET', True),
        (rf'{root}\get_STK_CASHFLOW_STATEMENT', True),
        (rf'{root}\get_STK_INCOME_STATEMENT', True),

        (rf'{root}\get_index_weights\000016.XSHG', False),
        (rf'{root}\get_index_weights\000300.XSHG', False),
        (rf'{root}\get_index_weights\000852.XSHG', False),
        (rf'{root}\get_index_weights\000905.XSHG', False),

        (rf'{root}\get_price_futures_daily', False),
        (rf'{root}\get_dominant_futures', False),

        (rf'{root}\get_index_daily', False),
    ]


def remove(path):
    files1 = files_to_dataframe(path, FILE_SUFFIX + TEMP_SUFFIX)
    if files1.empty:
        return
    files2 = files_to_dataframe(path, FILE_SUFFIX)
    remove_df = pd.merge(files1, files2, how='inner', on=['start', 'end'])
    if remove_df.empty:
        return

    for i, row in remove_df.iterrows():
        f = row['path_x']
        logger.info(f)
        f.unlink(missing_ok=True)


# 源数据
PATH_INPUT1 = r'D:\data\jqresearch'
if __name__ == '__main__':
    paths = get_paths(PATH_INPUT1)
    for path, _ in paths:
        remove(path)
