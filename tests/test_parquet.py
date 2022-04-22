import pathlib

import pandas as pd
import pyarrow as pa

print(pd.__version__)  # 1.4.1
print(pa.__version__)  # 7.0.0

dr = pd.date_range(start='2022-01-01', end='2022-01-10', freq='D')
df = pd.DataFrame(index=dr)
df['A'] = pd.to_datetime('today')
df['B'] = 1.0
df['C'] = 'a'
df['D'] = 2

df.head(1).to_parquet('b/0.parquet')  # not empty dateframe
df.head(0).to_parquet('b/1.parquet')  # empty dateframe
df.head(0).to_parquet('b/2.parquet')  # empty dateframe
df.to_parquet('b/3.parquet')
pd.read_parquet('b')

# 另一种加载文件的方法
import pandas as pd

df = pd.concat([pd.read_parquet(_) for _ in pathlib.Path('.').glob('20220*')])
