import pathlib

import numpy as np
import polars as pl
import polars.selectors as cs
from loguru import logger


def load_financial(path: str) -> pl.DataFrame:
    """加载财务数据"""
    return (
        pl.read_parquet(path, use_pyarrow=True)
        .rename({'pubDate': 'time'})
        .drop(['id', 'statDate.1'])
        .with_columns(pl.col('time', 'statDate').str.strptime(pl.Datetime, "%Y-%m-%d"))
    )


def adjust_financial(df: pl.DataFrame, by1: str = 'code', by2: str = 'time', by3: str = 'statDate') -> pl.DataFrame:
    """财务报表调整。为了与交易日对齐，已经过滤了以下情况

    1. 2019年报由于疫情，比2020年一季报公布还晚
    2. 年报和一季报常同一天出

    Notes
    -----
    已经调整过的报表不能再进行时序上的计算和处理。因为数据已经不满足Point In Time的要求

    """
    return (
        df
        .fill_null(np.nan)  # 防止某期无值，forward_fill时产生数据跨期的问题
        .filter((pl.col(by3).diff() > 0).fill_null(True).over(by1, order_by=[by2, by3]))  # 过滤更新历史报告
        .sort(by1, by2, by3)
        .unique(subset=[by1, by2], keep='last', maintain_order=True)  # 一天公布多期，只看最新一期
    )


def join_financial(df1: pl.DataFrame, df2: pl.DataFrame, by1: str = 'code', by2: str = 'time', by3: str = 'statDate'):
    """合并行情与财务表。请提前对财务表进行ttm等计算。因为之后再按报告期对齐很麻烦

    1. 同一天发布多期，需要正确排序最新一期
    2. 更新历史上的某一期，不能把他当成最新一期

    Parameters
    ----------
    df1:
        行情表
    df2:
        财务表
    by1
    by2
    by3

    """
    # 收盘后公布会显示第二天，一周7天都可能公布。同一天能公布多期
    df2 = adjust_financial(df2, by1, by2, by3)

    # 标记作表，然后全连接
    d1 = df1.with_columns(__left__=True).join(df2, on=[by1, by2], how='full', coalesce=True)
    # 财务数据forward_fill填充
    d0 = d1.sort(by1, by2).with_columns(
        pl.col(*df2.columns).forward_fill().over(by1, order_by=by2),
        # 周五收盘后公布，标记到周六，但没有周一行情承接，但不能直接丢弃，标记一下
        __last__=pl.col('__left__').is_last_distinct().over(by1, order_by=by2),
    )
    # xx = d0.filter(pl.col('code') == '002721.XSHE', pl.col('time') >= datetime(2023, 4, 20))
    d0 = d0.filter(pl.col('__left__').is_not_null() | pl.col('__last__'))
    d0 = d0.drop('__left__', '__last__').fill_nan(None)
    return d0


def step1(ROOT) -> pl.DataFrame:
    PATH_INPUT_1 = rf'{ROOT}\get_fundamentals_balance'
    PATH_INPUT_2 = rf'{ROOT}\get_fundamentals_cash_flow'
    PATH_INPUT_3 = rf'{ROOT}\get_fundamentals_income'
    # xx1 = load_financial(PATH_INPUT_1).filter(pl.col('code') == '603285.XSHG').sort('code', 'time')
    # xx2 = load_financial(PATH_INPUT_2).filter(pl.col('code') == '603285.XSHG').sort('code', 'time')

    df1 = adjust_financial(load_financial(PATH_INPUT_1)).with_columns(__mask1__=1)
    df2 = adjust_financial(load_financial(PATH_INPUT_2)).with_columns(__mask2__=2)
    df3 = adjust_financial(load_financial(PATH_INPUT_3)).with_columns(__mask4__=4)

    df7 = (
        df1
        .join(df2, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .join(df3, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .with_columns(__mask7__=pl.sum_horizontal('__mask1__', '__mask2__', '__mask4__'))
        .fill_null(np.nan)
    )
    except_report = df7.filter(pl.col('__mask7__') < 7)
    logger.warning('异常基础表 {} 条，请争取补全数据', len(except_report))

    PATH_INPUT_4 = rf'{ROOT}\get_fundamentals_indicator'
    df4 = adjust_financial(load_financial(PATH_INPUT_4)).with_columns(__mask8__=8)
    df15 = (
        df7
        .join(df4, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .with_columns(__mask15__=pl.sum_horizontal('__mask7__', '__mask8__'))
        .fill_null(np.nan)
    )
    except_report7 = df15.filter(pl.col('__mask7__').is_nan())
    except_report8 = df15.filter(pl.col('__mask8__').is_nan())
    logger.warning('缺失基础表 {} 条，缺失衍生表 {} 条，请争取补全数据', len(except_report7), len(except_report8))

    return df15.drop('__mask1__', '__mask2__', '__mask4__', '__mask7__', '__mask8__', '__mask15__')


def main():
    # 输出路径
    PATH_OUTPUT = r'F:\preprocessing'
    PATH_OUTPUT = pathlib.Path(PATH_OUTPUT)
    PATH_OUTPUT.mkdir(parents=True, exist_ok=True)

    logger.info('start process')
    ROOT = r"F:\data\jqresearch"

    df2 = step1(ROOT)
    df2 = df2.select(cs.all().shrink_dtype())
    df2 = df2.shrink_to_fit()

    logger.info('start write')
    df2.write_parquet(PATH_OUTPUT / 'data3.parquet')
    logger.info('done')
    print(df2.tail())

    # TODO 涉及的字段太多，建议只取自己需要的字段用来合并
    df1 = pl.read_parquet(PATH_OUTPUT / 'data2.parquet')
    logger.info('start join')
    df3 = join_financial(df1, df2)
    del df1
    del df2

    df3 = df3.select(cs.all().shrink_dtype())
    df3 = df3.shrink_to_fit()

    # 分成两个文件，只有最后一个文件需要上传到网络，方便同步数据
    df3.filter(pl.col('time') < pl.date(2025, 9, 1)).write_parquet(PATH_OUTPUT / 'data4.parquet')
    df3.filter(pl.col('time') >= pl.date(2025, 9, 1)).write_parquet(PATH_OUTPUT / 'data5.parquet')
    logger.info('done')
    print(df3.tail())


if __name__ == '__main__':
    main()
