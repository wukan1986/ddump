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
        .fill_null(np.nan)  # 防止某期无值，forward_fill时产生数据跨期的问题
    )


def pit_prepare(df: pl.DataFrame, by1: str = 'stock_code', by2: str = 'announce_date', by3: str = 'report_date') -> pl.DataFrame:
    """将原始的财务表根据发布日期重新扩展，满足以下条件：

    1. 根据股票和发布日分组
    2. 组内没有未来数据
    3. 组内的同一报告期数据只取最新

    """
    TMP = '__TMP__'
    DATE = '__DATE__'

    # 根据发布日期，笛卡尔扩展，过滤掉未来数据
    df1 = (
        df
        .select(pl.col(by1).alias(TMP), pl.col(by2).alias(DATE))
        .join_where(df, (pl.col(TMP) == pl.col(by1)) & (pl.col(DATE) >= pl.col(by2)))
        .drop(TMP)
    )

    # 过滤数据，同报告期只保留最新的一条
    df2 = (
        df1
        .sort(by1, DATE, by3, by2)
        .unique([by1, DATE, by3], keep="last", maintain_order=True)
    )
    return df2


def pit_calc(df: pl.DataFrame, by1: str = 'stock_code', by2: str = '__DATE__', by3: str = 'report_date') -> pl.DataFrame:
    """输入PIT分组的财务数据，组内可计算时序指标"""
    df1 = (
        df.with_columns(
            # TODO 补充其他各种时序指标，注意，不要少了`( ).over(by1, by2, order_by=by3)`
            net_profit_to_total_operate_revenue_ttm=(pl.col('net_profit').rolling_mean(4) / pl.col('total_operating_revenue').rolling_mean(4)).over(by1, by2, order_by=by3)
        )
    )
    return df1


def pit_last(df: pl.DataFrame, by1: str = 'stock_code', by2: str = '__DATE__', by3: str = 'report_date') -> pl.DataFrame:
    """输入PIT分组的财务数据，每组只取最后一条"""
    df1 = (
        df
        .sort(by1, by2, by3)
        .unique([by1, by2], keep="last", maintain_order=True)
    )
    return df1


def join_financial(df1: pl.DataFrame, df2: pl.DataFrame, by1: str = 'code', by2: str = 'time'):
    """合并行情与财务表。请提前对财务表进行ttm等计算。因为之后再按报告期对齐很麻烦

    1. 同一天发布多期，需要正确排序最新一期
    2. 更新历史上的某一期，不能把他当成最新一期

    Parameters
    ----------
    df1:
        行情表
    df2:
        财务表。收盘后公布会显示第二天，一周7天都可能公布。同一天能公布多期
    by1
    by2

    """
    df1 = df1.sort(by1, by2)
    df2 = df2.sort(by1, by2)
    df3 = df1.join_asof(df2, left_on=by2, right_on=by2, by=by1, strategy="backward", check_sortedness=False)

    return df3


def step1(ROOT) -> pl.DataFrame:
    PATH_INPUT_1 = rf'{ROOT}\get_fundamentals_balance'
    PATH_INPUT_2 = rf'{ROOT}\get_fundamentals_cash_flow'
    PATH_INPUT_3 = rf'{ROOT}\get_fundamentals_income'

    df1 = load_financial(PATH_INPUT_1).with_columns(__mask1__=1)
    df2 = load_financial(PATH_INPUT_2).with_columns(__mask2__=2)
    df3 = load_financial(PATH_INPUT_3).with_columns(__mask4__=4)

    df7 = (
        df1
        .join(df2, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .join(df3, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .with_columns(__mask7__=pl.sum_horizontal('__mask1__', '__mask2__', '__mask4__'))
        .fill_null(np.nan)
    )
    except_report = df7.filter(pl.col('__mask7__') < 7).select('code', 'time', 'statDate', '__mask1__', '__mask2__', '__mask4__', '__mask7__')
    logger.warning('异常基础表 {} 条，请争取补全数据', len(except_report))

    PATH_INPUT_4 = rf'{ROOT}\get_fundamentals_indicator'
    df4 = load_financial(PATH_INPUT_4).with_columns(__mask8__=8)
    df15 = (
        df7
        .join(df4, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .with_columns(__mask15__=pl.sum_horizontal('__mask7__', '__mask8__'))
        .fill_null(np.nan)
    )
    except_report7 = df15.filter(pl.col('__mask7__').is_nan()).select('code', 'time', 'statDate', '__mask7__', '__mask8__', '__mask15__')
    except_report8 = df15.filter(pl.col('__mask8__').is_nan()).select('code', 'time', 'statDate', '__mask7__', '__mask8__', '__mask15__')
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

    # ======================================
    # Point In Time处理+时序指标计算

    df = pit_prepare(df2, by1='code', by2='time', by3='statDate')
    df = pit_calc(df, by1='code', by2='__DATE__', by3='statDate')
    df2 = pit_last(df, by1='code', by2='__DATE__', by3='statDate').drop('__DATE__')
    del df

    # ======================================

    # TODO 涉及的字段太多，建议只取自己需要的字段用来合并
    df1 = pl.read_parquet(PATH_OUTPUT / 'data2.parquet')
    logger.info('start join')
    df3 = join_financial(df1, df2)
    del df1
    del df2

    df3 = df3.select(cs.all().shrink_dtype())
    df3 = df3.shrink_to_fit()

    # 分成两个文件，只有最后一个文件需要上传到网络，方便同步数据
    df3.filter(pl.col('time') < pl.date(2025, 9, 1)).sort("code", "time").write_parquet(PATH_OUTPUT / 'data4_.parquet')
    df3.filter(pl.col('time') >= pl.date(2025, 9, 1)).sort("code", "time").write_parquet(PATH_OUTPUT / 'data5.parquet')
    logger.info('done')
    print(df3.tail())


if __name__ == '__main__':
    main()
