"""
以下代码必须要开启一个Notebook，获取并指定kernel_id，否则pwd=/，导致导入失败
"""

from jupyter_data_fetch.codec import LazyKernel


def get_fundamentals_balance(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_fundamentals_balance({repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_cash_flow(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_fundamentals_cash_flow({repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_income(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_fundamentals_income({repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_indicator(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_fundamentals_indicator({repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_fundamentals_valuation(date=None, statDate=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_fundamentals_valuation({repr(date)}, {repr(statDate)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_STK_XR_XD(board_plan_pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_STK_XR_XD({repr(board_plan_pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_STK_INCOME_STATEMENT(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_STK_INCOME_STATEMENT({repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_STK_CASHFLOW_STATEMENT(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_STK_INCOME_STATEMENT({repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_STK_BALANCE_SHEET(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_STK_BALANCE_SHEET({repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


def get_STK_FIN_FORCAST(pub_date=None):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_STK_FIN_FORCAST({repr(pub_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))


# ===========================================
def get_dominant_futures_all(symbols, date, end_date):
    kernel = LazyKernel.get_kernel()
    codec = LazyKernel.get_codec()
    code = f"""
from jqresearch_query import *
_ = get_dominant_futures_all({repr(symbols)}, {repr(date)}, {repr(end_date)})"""
    return codec.extract_decode(kernel.execute(codec.generate_code(code, var_name='_'), store_history=False))
