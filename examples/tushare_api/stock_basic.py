from examples.tushare_api.config import DATA_ROOT, pro
from ddump.api.dump import Dump

"""
每次全查，L的频率可以高一些

"""
if __name__ == '__main__':
    path = DATA_ROOT / 'stock_basic'
    d = Dump(pro, path, ['exchange', 'list_status'])
    for exchange in ('SSE', 'SZSE'):
        for list_status in ('L', 'D', 'P'):
            d.set_parameters('stock_basic', exchange=exchange, list_status=list_status)
            if not d.exists(timeout=3600 * 12):
                d.download()
                d.save(save_empty=True)
