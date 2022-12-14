"""

"""
import time

import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

from ddump.api.common import get_last_file
from ddump.api.dump import Dump__date
from examples.akshare.config import DATA_ROOT, ak


@retry(wait=wait_fixed(15), stop=stop_after_attempt(5))
def main():
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # end = '2022-11-04'
    date = pd.to_datetime(end)

    # 加载交易日历
    board = pd.read_parquet(get_last_file(DATA_ROOT / 'stock_board_industry_name_em', 'parquet'))

    for func_name in [
        "stock_board_industry_cons_em",  # 东方财富-成份股
    ]:
        for i, row in board.iterrows():
            # 这里是用 板块代码 ，还是用 板块名称 呢？
            path = DATA_ROOT / func_name / row['板块名称']
            d = Dump__date(ak, path, 'date')
            d.set_parameters(func_name, date=date, symbol=row['板块名称'])
            if not d.exists(file_timeout=3600 * 0.5, data_timeout=86400 * 2):
                d.download(kw=['symbol'])
                d.save(save_empty=True)
                # 这个地方按情况调整
                if i % 10 == 1:
                    time.sleep(10)


if __name__ == '__main__':
    main()
