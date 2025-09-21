"""
部分老数据很早以前下载，当时没有考虑到Index部分混乱问题
现在将老数据的Index重置一下

需手工修改路径，以后基本就不用动了
"""
import pandas as pd


def update_index(path, update=False):
    df1 = pd.read_parquet(path)
    if isinstance(df1.index, pd.RangeIndex):
        print(df1.index.is_monotonic_increasing, df1.index.is_monotonic_decreasing)
        return
    if pd.api.types.is_integer_dtype(df1.index.dtype) and df1.index.name is None:
        print(df1.head(2))
        print(df1.index.is_monotonic_increasing, df1.index.is_monotonic_decreasing)
        if update:
            df2 = df1.reset_index(drop=True)
            del df1
            df2.to_parquet(path, compression='zstd')


import pathlib

# 修改路径观察指定文件夹
files = pathlib.Path(r"F:\data\jqresearch\get_extras_stock_is_st").glob("*.parquet")
# 先观察数据是否要改，然后改成True进行更新
update = False

for i, file in enumerate(files):
    print(file)
    update_index(file, update=update)
    if not update:
        if i > 20:
            break
