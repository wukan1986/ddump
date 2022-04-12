import pathlib


def test_rename():
    path = pathlib.Path('.')
    for f in path.glob('*'):
        if not f.is_dir():
            continue
        if f.name.endswith(('_0', '_1', '_2')):
            b = f.name[:-1] + '_' + f.name[-1]
            # print(b)
            print(f.rename(b))
