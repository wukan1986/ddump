from datetime import datetime

import pandas as pd

from ddump.api.dump import Dump__date
from examples.jqresearch.config import DATA_ROOT, jqr

"""
除权除息数据下载

注意，分红时间的报告期不一定是按季出现
"""


def main():
    for func_name in [
        "get_STK_XR_XD",
    ]:
        path = DATA_ROOT / func_name
        d = Dump__date(jqr, path, 'end_date')
        # 前半段，按周查，这样能快一些
        end = pd.to_datetime(datetime.today().date()) + pd.Timedelta(days=91)
        start = pd.to_datetime('2014-12-20')
        start = pd.to_datetime('2023-12-20')

        for dr in pd.date_range(start=start, end=end, freq='QE'):
            q = f'{dr:%Y-%m-%d}'
            d.set_parameters(func_name,
                             end_date=dr,
                             board_plan_pub_date=q)
            if not d.exists(file_timeout=3600 * 6, data_timeout=86400 * 90):
                d.download(kw=['board_plan_pub_date'])
                d.save()


if __name__ == '__main__':
    main()
