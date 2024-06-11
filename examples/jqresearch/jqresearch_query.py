# 此文件需复制到被控端, 当前前工作目录下，如Notebook同目录等
# jqresearch.api.get_fundamentals
# 只能以同步方式调用
import pandas as pd

from jqresearch.api import *  # noqa


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


def get_fundamentals_valuation(date=None, statDate=None):
    """市值数据，每日更新"""
    dfs = []
    last_id = -1
    while True:
        q = (
            query(valuation)
            .filter(valuation.id > last_id)
            .order_by(valuation.id)
            .limit(4000)
        )

        df = get_fundamentals(q, date=date, statDate=statDate)
        if df.empty:
            break
        last_id = df['id'].iloc[-1]
        dfs.append(df)

    if len(dfs) == 0:
        return pd.DataFrame()
    else:
        return pd.concat(dfs).reset_index(drop=True)


def _get_finance_xr_xd(F, FT, board_plan_pub_date=None):
    """提取财务表"""
    end_date = board_plan_pub_date
    start_date = pd.to_datetime(end_date) - pd.Timedelta(days=80)
    start_date = f'{start_date:%Y-%m-01}'

    dfs = []
    last_id = -1
    while True:
        q = (
            query(FT)
            .filter(
                # 601088在2017年第一季度有特别分红
                # 其它表中是否有也不清楚，也按同样风格写，保险一些
                FT.board_plan_pub_date >= start_date,
                FT.board_plan_pub_date <= end_date,
                FT.id > last_id)
            .order_by(FT.id)
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
        # reset_index(drop=True)解决版本不同时No module named 'pandas.core.indexes.numeric'
        return pd.concat(dfs).reset_index(drop=True)


def _get_finance_table(F, FT, pub_date=None):
    """提取财务表"""
    end_date = pub_date
    start_date = pd.to_datetime(end_date) - pd.Timedelta(days=80)
    start_date = f'{start_date:%Y-%m-01}'

    dfs = []
    last_id = -1
    while True:
        q = (
            query(FT)
            .filter(
                # 其它表中是否有也不清楚，也按同样风格写，保险一些
                FT.pub_date >= start_date,
                FT.pub_date <= end_date,
                FT.id > last_id)
            .order_by(FT.id)
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
        # reset_index(drop=True)解决版本不同时No module named 'pandas.core.indexes.numeric'
        return pd.concat(dfs).reset_index(drop=True)


def get_STK_XR_XD(board_plan_pub_date=None):
    """除权除息，原函数一次只能取4000，所以需要添加特别处理

    601088在2017年第一季度有特别分红
    """
    from jqdata import finance as F  # noqa

    return _get_finance_xr_xd(F, F.STK_XR_XD, board_plan_pub_date)


def get_STK_INCOME_STATEMENT(pub_date=None):
    """合并利润表"""
    from jqdata import finance as F  # noqa

    return _get_finance_table(F, F.STK_INCOME_STATEMENT, pub_date)


def get_STK_CASHFLOW_STATEMENT(pub_date=None):
    """合并现金流量表"""
    from jqdata import finance as F  # noqa

    return _get_finance_table(F, F.STK_CASHFLOW_STATEMENT, pub_date)


def get_STK_BALANCE_SHEET(pub_date=None):
    """合并资产负债表"""
    from jqdata import finance as F  # noqa

    return _get_finance_table(F, F.STK_BALANCE_SHEET, pub_date)


# ===========================================
def get_dominant_futures_all(symbols, date, end_date):
    """批量获取主力合约代码"""
    from jqdata.apis import get_dominant_futures  # noqa

    df = {}
    for symbol in symbols:
        df[symbol] = get_dominant_futures(symbol, date, end_date)
    return pd.DataFrame(df)
