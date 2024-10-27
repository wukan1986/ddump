from pathlib import Path

import polars as pl
import polars.selectors as cs
from expr_codegen.tool import codegen_exec
from loguru import logger
from polars_ta.wq import *

# 输入路径
PATH_INPUT1 = r'M:\preprocessing\data1.parquet'
# 输出路径
PATH_OUTPUT = r'M:\preprocessing'
PATH_OUTPUT = Path(PATH_OUTPUT)
PATH_OUTPUT.mkdir(parents=True, exist_ok=True)


def _code_block_1():
    # 不能跳过停牌的相关信息。如成份股相关处理

    # 注意：收益没有减1，停牌时值为1。也没有平移
    ROCR = CLOSE / ts_delay(CLOSE, 1)

    # 不少成份股数据源每月底更新，而不是每天更新，所以需要用以下方法推算
    # 注意1：在成份股调整月，如果缺少调整日的权重信息当月后一段的数据不准确
    # 注意2：不在成份股的权重要为0，否则影响之后计算，所以停牌也得保留
    SSE50 = cs_scale(ts_zip_prod(cs_fill_zero(sz50), ROCR), 100)
    CSI300 = cs_scale(ts_zip_prod(cs_fill_zero(hs300), ROCR), 100)
    CSI500 = cs_scale(ts_zip_prod(cs_fill_zero(zz500), ROCR), 100)
    CSI1000 = cs_scale(ts_zip_prod(cs_fill_zero(zz1000), ROCR), 100)


def _code_block_2():
    # 跳过停牌的相关指标

    # 这里用未复权的价格更合适
    DOJI4 = four_price_doji(open, high, low, close)
    # 明日停牌
    NEXT_DOJI4 = ts_delay(DOJI4, -1)


def step1() -> pl.DataFrame:
    # 调整字段名
    df = pl.read_parquet(PATH_INPUT1)
    df = df.rename({'time': 'date', 'code': 'asset', 'money': 'amount'})

    df = df.with_columns([
        # 成交均价，未复权
        (pl.col('amount') / pl.col('volume')).alias('vwap'),
    ])

    df = df.with_columns([
        # 后复权
        (pl.col(['open', 'high', 'low', 'close', 'vwap']) * pl.col('factor')).name.map(lambda x: x.upper()),
    ])
    return df


def step2(df: pl.DataFrame) -> pl.DataFrame:
    df = df.fill_nan(None)

    df = codegen_exec(df, _code_block_1).filter(~pl.col('paused'))
    df = codegen_exec(df, _code_block_2)

    df = df.drop(['sz50', 'hs300', 'zz500', 'zz1000'])
    # 计算出来的结果需要进行部分修复，防止之后计算时出错
    df = df.with_columns(pl.col('NEXT_DOJI4').fill_null(False))
    # 将计算结果中的inf都换成null
    df = df.with_columns(fill_nan(purify(cs.numeric())))
    return df


# %%
if __name__ == '__main__':
    # %%
    logger.info('start')
    df = step1()
    df = step2(df)

    df.write_parquet(PATH_OUTPUT / 'data2.parquet')

    logger.info('done')
    print(df.tail())
