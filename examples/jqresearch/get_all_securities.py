from ddump.api.dump import Dump
from examples.jqresearch.config import jq, DATA_ROOT


def main():
    path = DATA_ROOT / 'get_all_securities'
    for types in ['stock', 'futures', 'index']:
        d = Dump(jq, path, ['types'])  # ‘stock’, ‘fund’, ‘index’, ‘futures’, ‘etf’, ‘lof’, ‘fja’, ‘fjb’
        d.set_parameters('get_all_securities', types=types)
        if not d.exists(timeout=3600 * 1):
            d.download(kw=['types'])
            d.save()


if __name__ == '__main__':
    main()
