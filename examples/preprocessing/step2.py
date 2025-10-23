"""
添加涨停，明日一字
每日成分股权重等信息
"""
from pathlib import Path

import polars as pl
import polars.selectors as cs
from expr_codegen import codegen_exec
from loguru import logger
from polars_ta.wq import purify


def _code_block_1():
    # 不能跳过停牌的相关信息。如成份股相关处理

    # 注意：收益没有减1，停牌时值为1。也没有平移
    ROCR = CLOSE / CLOSE[1]

    # 不少成份股数据源每月底更新，而不是每天更新，所以需要用以下方法推算
    # 注意1：在成份股调整月，如果缺少调整日的权重信息当月后一段的数据不准确
    # 注意2：不在成份股的权重要为0，否则影响之后计算，所以停牌也得保留
    SSE50 = cs_scale(ts_cum_prod_by(ROCR, cs_fill_except_all_null(sz50)), 100)
    CSI300 = cs_scale(ts_cum_prod_by(ROCR, cs_fill_except_all_null(hs300)), 100)
    CSI500 = cs_scale(ts_cum_prod_by(ROCR, cs_fill_except_all_null(zz500)), 100)
    CSI1000 = cs_scale(ts_cum_prod_by(ROCR, cs_fill_except_all_null(zz1000)), 100)


def _code_block_2():
    # 跳过停牌的相关指标

    # 这里用未复权的价格更合适
    DOJI4 = four_price_doji(open, high, low, close)
    # 明日涨跌停
    NEXT_DOJI4 = DOJI4[-1]


def calc_factor1(df: pl.DataFrame,
                 by1: str = 'stock_code', by2: str = 'time',
                 close: str = 'close', pre_close: str = 'pre_close') -> pl.DataFrame:
    """计算复权因子，乘除法。使用交易所发布的昨收盘价计算

    Parameters
    ----------
    df : pl.DataFrame
        数据
    by1 : str
        分组字段
    by2 : str
        排序字段
    close : str
        收盘价字段
    pre_close : str
        昨收盘价字段

    Notes
    -----
    不关心是否真发生了除权除息过程，只要知道前收盘价和收盘价不等就表示发生了除权除息

    """
    df = (
        df
        .sort(by1, by2)
        .with_columns(
            factor1=(pl.col(close).shift(1, fill_value=pl.first(pre_close)) / pl.col(pre_close)).round(8).over(
                by1, order_by=by2))
        .with_columns(factor2=(pl.col('factor1').cum_prod()).over(by1, order_by=by2))
    )
    return df


def step1(PATH_INPUT1) -> pl.DataFrame:
    # 调整字段名
    df = pl.read_parquet(PATH_INPUT1)
    # df = df.rename({'time': 'date', 'code': 'asset', 'money': 'amount'})
    df = df.rename({'money': 'amount'})

    df = df.with_columns(
        # 成交均价，未复权
        vwap=pl.col('amount') / pl.col('volume')
    )

    df = calc_factor1(df, by1='code', by2='time', close='close', pre_close='pre_close')

    df = df.with_columns(
        # 后复权
        (pl.col(['open', 'high', 'low', 'close', 'vwap']) * pl.col('factor2')).name.map(lambda x: x.upper()),
    )
    df = df.with_columns(
        上海主板=pl.col('code').str.starts_with('60'),
        深圳主板=pl.col('code').str.starts_with('00'),
        科创板=pl.col('code').str.starts_with('68'),
        创业板=pl.col('code').str.starts_with('30'),
        北交所=pl.col('code').str.starts_with('8') | pl.col('code').str.starts_with('4') | pl.col('code').str.starts_with('9'),
    )
    return df


def step2(df: pl.DataFrame) -> pl.DataFrame:
    df = df.fill_nan(None)

    df = codegen_exec(df, _code_block_1, asset='code', date='time', over_null=None)#.filter(pl.col('paused') == 0)
    df = codegen_exec(df, _code_block_2, asset='code', date='time', over_null="partition_by")

    df = df.drop(['sz50', 'hs300', 'zz500', 'zz1000'])
    # 计算出来的结果需要进行部分修复，防止之后计算时出错
    df = df.with_columns(pl.col('NEXT_DOJI4').fill_null(False))
    # 将计算结果中的inf都换成null
    df = df.with_columns(purify(cs.numeric()))
    return df


def main():
    # 输出路径
    PATH_OUTPUT = r'F:\preprocessing'
    PATH_OUTPUT = Path(PATH_OUTPUT)
    PATH_OUTPUT.mkdir(parents=True, exist_ok=True)

    logger.info('start')
    # 输入路径
    PATH_INPUT1 = r'F:\preprocessing\data1.parquet'
    df = step1(PATH_INPUT1)
    df = step2(df)

    # 最后才调整大小
    df = df.select(cs.all().shrink_dtype())
    df = df.shrink_to_fit()

    print(df.tail())
    df.write_parquet(PATH_OUTPUT / 'data2.parquet')

    logger.info('done')


# %%
if __name__ == '__main__':
    main()
