"""
将最后一个文件更名，防止再次下载
1. 每次下载时，最后一个文件可能由于源服务器没有更新导致过时
2. 如果没有每天更新，而是中间休息了几天，会导致最后一个文件以为是老文件，早就更新过了，导致没有得到更新

所以这里使用改名的方法备份一下

注意：只有更新频率高的才需要此功能

"""
from loguru import logger

from ddump.api.common import get_last_file
from ddump.common import FILE_SUFFIX, TEMP_SUFFIX


def get_paths(root):
    return [
        (rf'{root}\get_extras_stock_is_st', False),
        (rf'{root}\get_industry_stock', False),
        (rf'{root}\get_price_stock_factor', False),
        (rf'{root}\get_price_stock_daily', False),
        #
        # # ignore_index=True 表示合并时丢弃索引，因为索引不含有效信息
        # (rf'{root}\get_fundamentals_balance', True),
        # (rf'{root}\get_fundamentals_cash_flow', True),
        # (rf'{root}\get_fundamentals_income', True),
        # (rf'{root}\get_fundamentals_indicator', True),
        (rf'{root}\get_fundamentals_valuation', True),
        # (rf'{root}\get_STK_XR_XD', True),
        # (rf'{root}\get_STK_BALANCE_SHEET', True),
        # (rf'{root}\get_STK_CASHFLOW_STATEMENT', True),
        # (rf'{root}\get_STK_INCOME_STATEMENT', True),
        #
        # (rf'{root}\get_index_weights\000016.XSHG', False),
        # (rf'{root}\get_index_weights\000300.XSHG', False),
        # (rf'{root}\get_index_weights\000852.XSHG', False),
        # (rf'{root}\get_index_weights\000905.XSHG', False),

        (rf'{root}\get_price_futures_daily', False),
        (rf'{root}\get_dominant_futures', False),

        (rf'{root}\get_index_daily', False),
    ]


def rename(path):
    file = get_last_file(path, FILE_SUFFIX)
    if file is None:
        return
    logger.info(file)
    file.rename(file.with_suffix(FILE_SUFFIX + TEMP_SUFFIX))


def main():
    # 源数据
    PATH_INPUT1 = r'D:\data\jqresearch'
    paths = get_paths(PATH_INPUT1)
    for path, _ in paths:
        try:
            rename(path)
        except FileExistsError as e:
            logger.error(e)


if __name__ == '__main__':
    main()
