import pandas as pd

from examples.jqdatasdk.config import jq, DATA_ROOT
from ddump.api.dump import Dump__start__end


class Wrapper:

    @staticmethod
    def get_trade_days(start_date, end_date):
        """由于输出格式比较特特殊, 只能特意封装一下"""
        arr = jq.get_trade_days(start_date=start_date, end_date=end_date)
        return pd.DataFrame(pd.to_datetime(arr), columns=['date'])


if __name__ == '__main__':
    start = '2005-01-01'
    end = f"{pd.to_datetime('today'):%Y-%m-%d}"

    path = DATA_ROOT / 'get_trade_days'
    d = Dump__start__end(Wrapper, path, 'start_date', 'end_date')
    for dr in pd.date_range(start=start, end=end, freq='YS'):
        start_date = f'{dr:%Y}-01-01'
        end_date = f'{dr:%Y}-12-31'
        d.set_parameters('get_trade_days', start_date=start_date, end_date=end_date)
        if not d.exists(file_timeout=86400 - 3600, data_timeout=86400 * 10):
            d.download()
            d.save(save_empty=True)
