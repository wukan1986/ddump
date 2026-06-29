"""
老版，必须要开启一个Notebook，获取并指定kernel_id，否则pwd=/，导致导入服务端py失败

新版，将服务端py合并到当前文件，不再关注pwd, 不用制定kernel_id，用完就释放了，减少内存占用
"""

from jupyter_data_fetch.codec import LazyKernel

GET_FUNDAMENTALS_CODE = """
import pandas as pd
from jqresearch.api import get_fundamentals

def _get_fundamentals(T, date=None, statDate=None):
    dfs = []
    last_id = -1
    while True:
        q = (
            query(T)
            .filter(T.id > last_id)
            .order_by(T.id)
            .limit(4000)
        )

        df = get_fundamentals(q, date=date, statDate=statDate)
        if df.empty:
            break
        last_id = df['id'].iloc[-1]
        dfs.append(df)
        if len(df) < 4000:
            break

    if len(dfs) == 0:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)
"""


def get_fundamentals_balance(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FUNDAMENTALS_CODE}

_ = _get_fundamentals(balance, {repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_cash_flow(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FUNDAMENTALS_CODE}

_ = _get_fundamentals(cash_flow, {repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_income(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FUNDAMENTALS_CODE}

_ = _get_fundamentals(income, {repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_indicator(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FUNDAMENTALS_CODE}

_ = _get_fundamentals(indicator, {repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_valuation(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FUNDAMENTALS_CODE}

_ = _get_fundamentals(valuation, {repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


# ===========================================
GET_FINANCE_CODE = """
import pandas as pd

def _get_finance(F, FT, {0}=None):
    end_date = {0}
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=80)).strftime('%Y-%m-01')

    dfs = []
    last_id = -1
    while True:
        q = (
            query(FT)
            .filter(
                # 601088在2017年第一季度有特别分红
                # 其它表中是否有也不清楚，也按同样风格写，保险一些
                FT.{0} >= start_date,
                FT.{0} <= end_date,
                FT.id > last_id)
            .order_by(FT.id)
            .limit(4000)
        )
        df = F.run_query(q)
        if df.empty:
            break
        last_id = df['id'].iloc[-1]
        dfs.append(df)
        if len(df) < 4000:
            break

    if len(dfs) == 0:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)
"""


def get_STK_XR_XD(board_plan_pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FINANCE_CODE.format('board_plan_pub_date')}

from jqdata import finance as F  # noqa

_ = _get_finance(F, F.STK_XR_XD, {repr(board_plan_pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False, timeout=60))


def get_STK_INCOME_STATEMENT(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FINANCE_CODE.format('pub_date')}

from jqdata import finance as F  # noqa

_ = _get_finance(F, F.STK_INCOME_STATEMENT, {repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False, timeout=60))


def get_STK_CASHFLOW_STATEMENT(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FINANCE_CODE.format('pub_date')}

from jqdata import finance as F  # noqa

_ = _get_finance(F, F.STK_CASHFLOW_STATEMENT, {repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False, timeout=60))


def get_STK_BALANCE_SHEET(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FINANCE_CODE.format('pub_date')}

from jqdata import finance as F  # noqa

_ = _get_finance(F, F.STK_BALANCE_SHEET, {repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False, timeout=60))


def get_STK_FIN_FORCAST(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
{GET_FINANCE_CODE.format('pub_date')}

from jqdata import finance as F  # noqa

_ = _get_finance(F, F.STK_FIN_FORCAST, {repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False, timeout=60))


# ===========================================
def get_dominant_futures_all(symbols, date, end_date):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
import pandas as pd
from jqdata.apis import get_dominant_futures

def _get_dominant_futures(symbols, date, end_date):
    df = dict()
    for symbol in symbols:
        df[symbol] = get_dominant_futures(symbol, date, end_date)
    return pd.DataFrame(df)
    
_ = _get_dominant_futures({repr(symbols)}, {repr(date)}, {repr(end_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))
