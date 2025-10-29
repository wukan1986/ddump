"""
第一步处理，将多个文件合并成一个
做一些基本处理
"""
import pathlib

import polars as pl
from loguru import logger


def step1(ROOT) -> pl.DataFrame:
    # 日线行情
    PATH_INPUT1 = rf'{ROOT}\get_price_stock_daily'
    # IPO日期
    PATH_INPUT2 = rf'D:\data\jqresearch\get_all_securities\stock.parquet'
    # 是否ST
    PATH_INPUT3 = rf'{ROOT}\get_extras_stock_is_st'
    # 行业分类，申万一级
    PATH_INPUT4 = rf'{ROOT}\get_industry_stock'
    # 估值
    PATH_INPUT5 = rf'{ROOT}\get_fundamentals_valuation'
    # 主要的几个指数权重，如沪深300，中证500等，月底更新
    PATH_INPUT6 = rf'{ROOT}\get_index_weights\000016.XSHG'
    PATH_INPUT7 = rf'{ROOT}\get_index_weights\000300.XSHG'
    PATH_INPUT8 = rf'{ROOT}\get_index_weights\000905.XSHG'
    PATH_INPUT9 = rf'{ROOT}\get_index_weights\000852.XSHG'
    PATH_INPUT10 = rf'{ROOT}\get_index_weights\932000.CSI'

    # 多文件加载
    df1 = (
        pl.read_parquet(PATH_INPUT1, use_pyarrow=True)
        # .with_columns(pl.col('paused').cast(pl.Boolean))
        .sort(by=['code', 'time'])
    )

    df2 = (
        pl.read_parquet(PATH_INPUT2, use_pyarrow=True)
        .rename({'__index_level_0__': 'code', 'start_date': 'ipo_date'})
        .select('code', 'ipo_date')
        .with_columns(
            pl.col('ipo_date').cast(pl.Datetime('us'))
        )
    )
    df3 = pl.read_parquet(PATH_INPUT3, use_pyarrow=True, columns=['time', 'code', 'is_st'])
    # 行业分类
    df4 = (
        pl.read_parquet(PATH_INPUT4, use_pyarrow=True)
        .rename({'date': 'time', '__index_level_0__': 'code'})
        .filter(pl.col('is_code')).drop('is_code')
        .with_columns(
            pl.col('sw_l1', 'sw_l2', 'sw_l3').cast(pl.UInt32),
            pl.col('time').cast(pl.Datetime('us'))
        )
    )
    # 估值
    df5 = (
        pl.read_parquet(PATH_INPUT5, use_pyarrow=True)
        .rename({'day': 'time'})
        .drop(['id', 'pubDate'])
        .with_columns(pl.col('time').str.strptime(pl.Datetime, "%Y-%m-%d"))
    )

    # 指数权重
    df6 = (
        pl.read_parquet(PATH_INPUT6, use_pyarrow=True)
        .rename({'date': 'time', 'weight': 'sz50'})
        .with_columns(pl.col('time').cast(pl.Datetime('us')))
        .drop('display_name')
    )
    df7 = (
        pl.read_parquet(PATH_INPUT7, use_pyarrow=True)
        .rename({'date': 'time', 'weight': 'hs300'})
        .with_columns(pl.col('time').cast(pl.Datetime('us')))
        .drop('display_name')
    )
    df8 = (
        pl.read_parquet(PATH_INPUT8, use_pyarrow=True)
        .rename({'date': 'time', 'weight': 'zz500'})
        .with_columns(pl.col('time').cast(pl.Datetime('us')))
        .drop('display_name')
    )
    df9 = (
        pl.read_parquet(PATH_INPUT9, use_pyarrow=True)
        .rename({'date': 'time', 'weight': 'zz1000'})
        .with_columns(pl.col('time').cast(pl.Datetime('us')))
        .drop('display_name')
    )
    df10 = (
        pl.read_parquet(PATH_INPUT9, use_pyarrow=True)
        .rename({'date': 'time', 'weight': 'zz2000'})
        .with_columns(pl.col('time').cast(pl.Datetime('us')))
        .drop('display_name')
    )

    # 排序，合并时能提速
    df1 = df1.sort(by=['code', 'time'])
    # df2 = df2.sort(by=['code', 'time'])
    df3 = df3.sort(by=['code', 'time'])
    df4 = df4.sort(by=['code', 'time'])
    df5 = df5.sort(by=['code', 'time'])
    df6 = df6.sort(by=['code', 'time'])
    df7 = df7.sort(by=['code', 'time'])
    df8 = df8.sort(by=['code', 'time'])
    df9 = df9.sort(by=['code', 'time'])
    df10 = df10.sort(by=['code', 'time'])

    # 多表合并
    dd = (
        df1
        .join(df2, on=['code'], how='left', coalesce=True)
        .join(df3, on=['code', 'time'], how='left', coalesce=True)
        .join(df4, on=['code', 'time'], how='left', coalesce=True)
        .join(df5, on=['code', 'time'], how='left', coalesce=True)
        .join(df6, on=['code', 'time'], how='left', coalesce=True)
        .join(df7, on=['code', 'time'], how='left', coalesce=True)
        .join(df8, on=['code', 'time'], how='left', coalesce=True)
        .join(df9, on=['code', 'time'], how='left', coalesce=True)
        .join(df10, on=['code', 'time'], how='left', coalesce=True)
    ).with_columns(
        pl.col('is_st').fill_null(False),
        total_days=(pl.col('time') - pl.col('ipo_date')).dt.total_days() + 1
    )
    return dd


def step2(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort('code', 'time').with_columns(
        pl.col('sw_l1', 'sw_l2', 'sw_l3', 'zjw').forward_fill().backward_fill().over('code', order_by='time')
    )
    return df


def main():
    # 输出路径
    PATH_OUTPUT = r'F:\preprocessing'
    PATH_OUTPUT = pathlib.Path(PATH_OUTPUT)
    PATH_OUTPUT.mkdir(parents=True, exist_ok=True)

    logger.info('start process')
    ROOT = r"F:\data\jqresearch"

    df = step1(ROOT)
    df = step2(df)

    logger.info('start write')
    df.write_parquet(PATH_OUTPUT / 'data1.parquet')
    logger.info('done')
    print(df.tail())


if __name__ == '__main__':
    main()
