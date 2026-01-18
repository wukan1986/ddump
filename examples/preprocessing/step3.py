import pathlib

import polars as pl
import polars.selectors as cs
from loguru import logger
from polars_ta.utils.pit import join_quote_financial, pit_prepare, pit_frist, ANNOUNCE_DATE


def load_financial(path: str) -> pl.DataFrame:
    """加载财务数据"""
    return (
        pl.read_parquet(path, use_pyarrow=True)
        .rename({'pubDate': 'time'})
        .drop(['id', 'statDate.1'])
        .with_columns(pl.col('time', 'statDate').str.strptime(pl.Datetime, "%Y-%m-%d"))
        # .fill_null(np.nan)  # 防止某期无值，forward_fill时产生数据跨期的问题
    )


def pit_calc(df: pl.DataFrame, by1: str = 'stock_code', by2: str = ANNOUNCE_DATE, by3: str = 'report_date') -> pl.DataFrame:
    """输入PIT分组的财务数据，组内可计算时序指标"""
    df1 = (
        df.with_columns(
            # TODO 补充其他各种时序指标，注意，不要少了`( ).over(by1, by2, order_by=by3)`
            net_profit_to_total_operate_revenue_ttm=(pl.col('net_profit').rolling_mean(4) / pl.col('total_operating_revenue').rolling_mean(4)).over(by1, by2, order_by=by3)
        )
    )
    return df1


def step1(ROOT) -> pl.DataFrame:
    # 全是单季数据
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
    )
    except_report = df7.filter(pl.col('__mask7__') < 7).select('code', 'time', 'statDate', '__mask1__', '__mask2__', '__mask4__', '__mask7__')
    logger.warning('异常基础表 {} 条，请争取补全数据', len(except_report))

    PATH_INPUT_4 = rf'{ROOT}\get_fundamentals_indicator'
    df4 = load_financial(PATH_INPUT_4).with_columns(__mask8__=8)
    df15 = (
        df7
        .join(df4, on=['code', 'time', 'statDate'], how='full', coalesce=True)
        .with_columns(__mask15__=pl.sum_horizontal('__mask7__', '__mask8__'))
    )
    except_report7 = df15.filter(pl.col('__mask7__').is_null()).select('code', 'time', 'statDate', '__mask7__', '__mask8__', '__mask15__')
    except_report8 = df15.filter(pl.col('__mask8__').is_null()).select('code', 'time', 'statDate', '__mask7__', '__mask8__', '__mask15__')
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
    df = pit_calc(df, by1='code', by2=ANNOUNCE_DATE, by3='statDate')
    df2 = pit_frist(df, by1='code', by2='time', by3='statDate', by4=ANNOUNCE_DATE)
    del df

    # ======================================

    # TODO 涉及的字段太多，建议只取自己需要的字段用来合并
    df1 = pl.read_parquet(PATH_OUTPUT / 'data2.parquet')
    logger.info('start join')
    df3 = join_quote_financial(df1, df2, by1='code', by2='time')
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
