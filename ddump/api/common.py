import pathlib
import time
from functools import lru_cache

import pandas as pd

from ..common import START_SEP_END, FILE_SUFFIX


def start_end_2_name(start, end):
    """格式化start,end"""
    import datetime

    if isinstance(start, (pd.Timestamp, datetime.datetime)):
        start = f'{start:%Y%m%dT%H%M%S}'
    if isinstance(start, int):
        start = f'{start:020d}'

    if isinstance(end, (pd.Timestamp, datetime.datetime)):
        end = f'{end:%Y%m%dT%H%M%S}'
    if isinstance(end, int):
        end = f'{end:020d}'

    return f'{start}{START_SEP_END}{end}'


@lru_cache(maxsize=4)
def files_to_dataframe(path, suffix=FILE_SUFFIX):
    """目录中文件转DataFrame

    文件名有格式要求，用分隔符分成前后两个时间。时间为左闭右闭关系。

    Parameters
    ----------
    path: pathlib.Path
    suffix: str
        后缀

    Returns
    -------
    pd.DataFrame

    """
    path = pathlib.Path(path)
    files = list(path.glob(f'*{suffix}'))
    df = pd.DataFrame([f.name.split('.')[0].split(START_SEP_END) for f in files], columns=['start', 'end'])
    # 结束时间
    df['end_s'] = df['end'].apply(lambda x: pd.to_datetime(x).timestamp())
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
    df['path'] = files
    # 文件修改时间
    df['st_mtime'] = [x.stat().st_mtime for x in files]
    # 当前时间，需要立即使用，之后再用就不准了
    df['now'] = time.time()
    return df


def timeout_mtime(path, timeout):
    """检查文件超时"""
    return time.time() - pathlib.Path(path).stat().st_mtime > timeout


def filter_range_in_dataframe(df, start, end, file_timeout, data_timeout):
    """检查日期是否在某一个文件中

    日期可以跨多个文件

    单日期只是两个日期的特例

    Parameters
    ----------
    df
    start
    end
    file_timeout
    data_timeout

    Returns
    -------
    bool

    """
    if df is None:
        return pd.DataFrame()
    # 找到多条记录
    df = df[(df['start'] <= end) & (start <= df['end'])]
    # 太近了表示存在
    # 太远了，也表示存在
    df = df[(df['now'] - df['st_mtime'] < file_timeout) | (df['now'] - df['end_s'] > data_timeout)]
    return df


def get_last_file(path, suffix):
    """通过最新文件的文件名得到关键参数

    Parameters
    ----------
    path
    suffix

    Returns
    -------
    dt
    id

    """
    path = pathlib.Path(path)
    files = list(path.glob(f'*{suffix}'))
    if len(files) == 0:
        return None
    return files[-1]
