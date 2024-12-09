from ddump.api.dump import Dump
from examples.akshare.config import DATA_ROOT, ak

if __name__ == '__main__':
    path = DATA_ROOT / 'tool_trade_date_hist_sina'
    d = Dump(ak, path, ['filename'])
    d.set_parameters('tool_trade_date_hist_sina', filename='calendar')
    if not d.exists(timeout=86400 * 1):
        d.download(kw=[])
        d.save()
    print(d.load())
