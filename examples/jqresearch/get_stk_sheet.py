from datetime import datetime

import pandas as pd

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, jqr

"""
原始报表下载

资产负债表
利润表
现金流量表

如果数据下载是按报告期，这会导致要下载之前的数据基本上要全部重下一次，不现实
如果按发布期来算，就没有问题了
"""


def main():
    for func_name in [
        "get_STK_BALANCE_SHEET",
        "get_STK_INCOME_STATEMENT",
        "get_STK_CASHFLOW_STATEMENT",
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(jqr, path, 'end_date')
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=91)
        start = pd.to_datetime(datetime.today().date()) - pd.Timedelta(days=183)

        for dr in pd.date_range(start=start, end=end, freq='QE'):
            q = f'{dr:%Y-%m-%d}'
            d.set_parameters(func_name,
                             end_date=dr,
                             pub_date=q)
            if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 90):
                # print(dr, q)
                d.download(kw=['pub_date'])
                d.save()


if __name__ == '__main__':
    main()
