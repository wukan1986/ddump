# 此文件需复制到被控端, 当前前工作目录下，如Notebook同目录等
# jqresearch.api.get_fundamentals
# 只能以同步方式调用
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
