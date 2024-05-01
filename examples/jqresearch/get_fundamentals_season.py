from datetime import datetime

import pandas as pd

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, jqr

"""
单季数据下载
https://www.joinquant.com/help/api/help#Stock:获取单季度年度财务数据
https://www.joinquant.com/community/post/detailMobile?postId=2750  数据常见疑问汇总（持续汇总中...）

资产负债表来自于原表
而现金流量表和利润表做了单季处理

数据不能再做时序上的跨期处理，如求TTM等，因为只记录了第一条值，后面的更新都没有
如需跨期，需从原始表中开始推算
"""

if __name__ == '__main__':

    for func_name in [
        "get_fundamentals_balance",
        "get_fundamentals_cash_flow",
        "get_fundamentals_income",
        "get_fundamentals_indicator",
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(jqr, path, 'end_date')
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime('2023-01-15')
        end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=6)
        start = pd.to_datetime('2022-12-01')

        for dr in pd.date_range(start=start, end=end, freq='Q'):
            q = f'{dr.year}q{dr.month // 3}'
            d.set_parameters(func_name,
                             end_date=dr,
                             statDate=q)
            if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 150):
                d.download(kw=['statDate'])
                d.save()
