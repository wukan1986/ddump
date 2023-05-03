# 此文件需复制到被控端, 当前前工作目录下，如Notebook同目录等
# jqresearch.api.get_fundamentals
# 只能以同步方式调用
import pandas as pd


def get_fundamentals_valuation(date=None, statDate=None):
    """市值数据，每日更新"""
    q = query(
        valuation
    )
    return get_fundamentals(q, date=date, statDate=statDate)


def get_fundamentals_balance(date=None, statDate=None):
    """资产负债数据，按季更新"""
    q = query(
        balance
    )
    return get_fundamentals(q, date=date, statDate=statDate)


def get_fundamentals_cash_flow(date=None, statDate=None):
    """现金流数据，按季更新，单季"""
    q = query(
        cash_flow
    )
    return get_fundamentals(q, date=date, statDate=statDate)


def get_fundamentals_income(date=None, statDate=None):
    """利润数据，按季更新，单季"""
    q = query(
        income
    )
    return get_fundamentals(q, date=date, statDate=statDate)


def get_fundamentals_indicator(date=None, statDate=None):
    """财务指标数据，按季更新，单季"""
    q = query(
        indicator
    )
    return get_fundamentals(q, date=date, statDate=statDate)


def get_stk_xr_xd(report_date=None):
    """除权除息，原函数一次只能取4000，所以需要添加特别处理

    601088在2017年第一季度有特别分红
    """
    from jqdata import finance as F

    end_date = report_date
    start_date = pd.to_datetime(report_date) - pd.Timedelta(days=80)
    start_date = f'{start_date:%Y-%m-01}'

    dfs = []
    last_id = -1
    while True:
        q = (
            query(F.STK_XR_XD)
            .filter(
                F.STK_XR_XD.report_date >= start_date,
                F.STK_XR_XD.report_date <= end_date,
                F.STK_XR_XD.id > last_id)
            .order_by(F.STK_XR_XD.id)
            .limit(3000)
        )
        df = F.run_query(q)
        if df.empty:
            break
        last_id = df['id'].iloc[-1]
        dfs.append(df)

    if len(dfs) == 0:
        return pd.DataFrame()
    else:
        return pd.concat(dfs)
