import pandas as pd
from loguru import logger

from examples.jqdatasdk.config import jq, DATA_ROOT
from ddump.api.columns import binsearch_download, query_list_to_dataframe

if __name__ == '__main__':
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"
    path_parent = DATA_ROOT / 'get_index_stocks'
    path_parent.mkdir(parents=True, exist_ok=True)

    for symbol, start_date in [
        ('000016.XSHG', '2004-01-02'),  # 上证50
        ('000300.XSHG', '2005-04-08'),  # 沪深300
        ('000905.XSHG', '2007-01-15'),  # 中证500
        ('399006.XSHE', '2010-06-01'),  # 创业板指
    ]:
        path = path_parent / f'{symbol}.parquet'
        logger.info('开始处理 {}', path)
        df = binsearch_download(lambda x: query_list_to_dataframe(jq.get_index_stocks, x, symbol),
                                lambda: pd.read_parquet(path),
                                lambda x: x.to_parquet(path, compression='gzip'),
                                start_date, end)
