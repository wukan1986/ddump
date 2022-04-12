import pandas as pd

from examples.jqdatasdk.config import jq, DATA_ROOT
from ddump.api.dump import Dump__start__end

"""
演示按年下载历史数据

可能出现不同成份股行情重复下载问题
所以应当全A下载，然后根据成份股去拼接想要的数据

"""

if __name__ == '__main__':
    for func_name in [
        'get_price',
    ]:
        for symbol in ['000300.XSHG']:
            # 读取数据转成bool型
            universe = pd.read_parquet(DATA_ROOT / 'get_index_stocks' / f'{symbol}.parquet')
            universe = universe.astype(bool)

            path = DATA_ROOT / f'{func_name}_1' / symbol

            d = Dump__start__end(jq, path, 'start_date', 'end_date')
            for dr in pd.date_range(start='2005-01-01', end='2022-01-01', freq='YS'):
                start_date = f'{dr:%Y}-01-01'
                end_date = f'{dr:%Y}-12-31'
                # 只要一列中出现True就得下载
                select = universe[start_date:end_date].any(axis=0)
                select = select[select]
                d.set_parameters(func_name,
                                 start_date=start_date, end_date=end_date,
                                 security=select.index.tolist(), fq=None, panel=False)
                if not d.exists(file_timeout=86400 * 1, data_timeout=86400 * 10):
                    d.download()
                    d.save(save_empty=True)
