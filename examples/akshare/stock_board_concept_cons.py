"""
只能查最新的，无历史。可以每天更新此文件，这样即可获得每支股票今天的热点
"""
import time

import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

from ddump.api.common import get_last_file
from ddump.api.dump import Dump
from examples.akshare.config import DATA_ROOT, ak


def save_func_stock_board_industry_cons_em(df, date, board, board_code):
    df['date'] = date
    df['board'] = board
    df['board_code'] = board_code
    return df


@retry(wait=wait_fixed(15), stop=stop_after_attempt(5))
def main():
    end = f"{pd.to_datetime('today') - pd.Timedelta(hours=15, minutes=30):%Y-%m-%d}"
    # end = '2022-11-04'
    date = pd.to_datetime(end)

    # 加载行业板块
    board = pd.read_parquet(get_last_file(DATA_ROOT / 'stock_board_concept_name_em', 'parquet'))

    for func_name in [
        "stock_board_concept_cons_em",  # 东方财富-成份股
    ]:
        for i, row in board.iterrows():
            # 这里是用 板块代码 ，还是用 板块名称 呢？
            path = DATA_ROOT / func_name / f'{date:%Y%m%d}'
            d = Dump(ak, path, ['symbol'])
            d.set_parameters(func_name, date=date, symbol=row['板块名称'])
            if not d.exists(timeout=86400 * 0.5):
                d.download(kw=['symbol'])
                d.save(
                    pre_save=save_func_stock_board_industry_cons_em,
                    pre_save_kwargs={'date': date, 'board': row['板块名称'], 'board_code': row['板块代码']})
                # 这个地方按情况调整
                if i % 10 == 1:
                    time.sleep(10)


if __name__ == '__main__':
    main()
