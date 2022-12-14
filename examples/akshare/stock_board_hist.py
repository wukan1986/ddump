import time

import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

from ddump.api.common import get_last_file
from ddump.api.dump import Dump__start__end
from examples.akshare.config import DATA_ROOT, ak


@retry(wait=wait_fixed(15), stop=stop_after_attempt(5))
def main():
    # 第一次下载，下到去年
    end_date = pd.to_datetime('2021-12-31')
    start_date = pd.to_datetime('2000-01-01')

    # 之后的每次更新，从年初开始
    end_date = pd.to_datetime('2022-12-31')
    start_date = pd.to_datetime('2022-01-01')

    # 加载行业板块
    board = pd.read_parquet(get_last_file(DATA_ROOT / 'stock_board_industry_name_em', 'parquet'))

    func_name = 'stock_board_industry_hist_em'
    for i, row in board.iterrows():
        path = DATA_ROOT / func_name / row['板块名称']
        d = Dump__start__end(ak, path, 'start_date', 'end_date')

        d.set_parameters(func_name,
                         start_date=f'{start_date:%Y%m%d}', end_date=f'{end_date:%Y%m%d}',
                         symbol=row['板块名称'])
        if not d.exists(file_timeout=3600 * 12, data_timeout=86400 * 10):
            d.download()
            d.save(save_empty=True)
            # 这个地方按情况调整
            if i % 8 == 1:
                time.sleep(5)


if __name__ == '__main__':
    main()
