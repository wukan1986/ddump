from ddump.api.dump import Dump
from examples.akshare.config import DATA_ROOT, ak

if __name__ == '__main__':
    path = DATA_ROOT / 'tool_trade_date_hist_sina'
    d = Dump(ak, path, [])
    d.set_parameters('tool_trade_date_hist_sina')
    d.set_file_path('calendar')
    if not d.exists(timeout=86400 * 15):
        d.download()
        d.save(save_empty=True)
