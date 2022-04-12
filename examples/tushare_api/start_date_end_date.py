import pandas as pd

from examples.tushare_api.config import DATA_ROOT, pro
from ddump.api.dump import Dump__start__end

"""
# namechange
start_date/end_date是根据ann_date来的，但早期记录没有ann_date，所以只能按个股查询

"""

if __name__ == '__main__':
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"

    for func_name, start in [
        ('new_share', '2008-01-01'),  # 2013年没有新股记录
        ('namechange', '2010-01-01'),
    ]:
        path = DATA_ROOT / func_name
        d = Dump__start__end(pro, path, 'start_date', 'end_date')
        for dr in pd.date_range(start=start, end=end, freq='YS'):
            start_date = f'{dr:%Y}0101'
            end_date = f'{dr:%Y}1231'
            d.set_parameters(func_name, start_date=start_date, end_date=end_date)
            if not d.exists(file_timeout=86400 * 1, data_timeout=86400 * 10):
                d.download()
                d.save(save_empty=True)
