from examples.tushare.config import DATA_ROOT, pro
from ddump.api.dump import Dump

if __name__ == '__main__':
    path = DATA_ROOT / 'trade_cal'
    d = Dump(pro, path, ['exchange'])
    for exchange in ('SSE', 'SZSE'):
        d.set_parameters('trade_cal', exchange=exchange)
        if not d.exists(timeout=86400 * 15):
            d.download()
            d.save(save_empty=True)
