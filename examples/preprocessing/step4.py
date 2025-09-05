"""

TODO 对于股息率计算是否正确？

查了不少地方，一个是按哪一天作为分界点，有个能忽然两点分到了同一年，导致股息率翻倍，过一两天又恢复

是按市值还是按收盘价，正好跨复权点，股息率是怎样的

https://caifuhao.eastmoney.com/news/20241031155753066725480
https://www.joinquant.com/help/api/doc?name=JQDatadoc&id=10022&keyword=bonus_ratio_rmb

"""
import pathlib

import polars as pl


def read_parquets(path: pathlib.Path | str) -> pl.DataFrame:
    files = pathlib.Path(path).glob('*.parquet')
    dfs = []
    for f in files:
        dfs.append(pl.read_parquet(f))
    return pl.concat(dfs, how="vertical_relaxed")


def step0(path1, path2):
    # %% 加载数据

    # 有未来要分红的数据
    df1 = read_parquets(path1)
    df1 = df1.filter(pl.col('a_xr_date').is_not_null()).with_columns(pl.col('bonus_ratio_rmb').fill_null(0))
    df1 = (
        df1.select('code', 'bonus_ratio_rmb', 'bonus_amount_rmb',
                   pl.col('a_xr_date').cast(pl.Datetime('us')),
                   pl.col('report_date').cast(pl.Datetime('us')),
                   )
        .sort('code', 'a_xr_date')
    )
    x = df1.filter(pl.col('code') == '601398.XSHG')

    df2 = pl.read_parquet(path2, columns=['code', 'time', 'close', 'market_cap'])
    df = (
        df2.join(df1, how='full', left_on=['code', 'time'], right_on=['code', 'a_xr_date'], coalesce=True)
        .with_columns(pl.col('bonus_ratio_rmb', 'bonus_amount_rmb').fill_null(0.0))
    )
    del df1
    del df2

    df3 = (
        df.sort('code', 'time')
        .rolling(index_column='time', period='1y', group_by='code')
        .agg(
            pl.col('bonus_ratio_rmb').sum(), pl.col('bonus_amount_rmb').sum(),
            start=pl.min('time'), end=pl.max('time'), close=pl.last('close'), market_cap=pl.last('market_cap'),
        )
    )
    df3 = df3.with_columns(
        dividend_yield_ratio_1=pl.col('bonus_ratio_rmb') / 10 / pl.col('close'),
        dividend_yield_ratio_2=pl.col('bonus_amount_rmb') / (pl.col('market_cap') * 10000),
    )
    return df3


def main():
    # 输出路径
    PATH_OUTPUT = r'F:\preprocessing'
    PATH_OUTPUT = pathlib.Path(PATH_OUTPUT)
    PATH_OUTPUT.mkdir(parents=True, exist_ok=True)

    PATH_STEP0_INPUT1 = r'F:\data\jqresearch\get_STK_XR_XD'
    df = step0(PATH_STEP0_INPUT1, r'F:\preprocessing\data1.parquet')

    x = df.filter(pl.col('code') == '601398.XSHG')

    df.write_parquet(PATH_OUTPUT / 'data5.parquet')


if __name__ == '__main__':
    main()
