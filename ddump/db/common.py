import pandas as pd

from ..common import FILE_SUFFIX, KEY_SEP_ID


def last_key_id(df, str_key, str_id):
    """获取DataFrame中用于增量更新的两个字段最新值

    Parameters
    ----------
    df: pd.DataFrame
    str_key: str
    str_id: str

    Returns
    -------
    last_key: datetime
    last_id: int

    """
    last_key = df[str_key].iloc[-1]
    last_id = df[str_id].iloc[-1]
    return last_key, last_id


def max_key_id(df, str_key, str_id):
    """查找最大值，用于增量排序

    Parameters
    ----------
    df: pd.DataFrame
    str_key: str
    str_id: str

    Returns
    -------
    last_dt: datetime
    last_id: int

    """
    return last_key_id(df.sort_values(by=[str_key, str_id]),
                       str_key, str_id)


def key_id_2_name(last_key, last_id):
    """格式化dt与id"""
    import datetime

    if isinstance(last_key, (pd.Timestamp, datetime.datetime)):
        last_key = f'{last_key:%Y%m%dT%H%M%S}'
    if isinstance(last_key, int):
        last_key = f'{last_key:020d}'
    return f'{last_key}{KEY_SEP_ID}{last_id:020d}'


def name_2_dt_id(name, suffix):
    """从文件名中获取dt和id"""
    name = name.rstrip(suffix)
    last_key, last_id = tuple(name.split(KEY_SEP_ID))
    return last_key, int(last_id)


def get_last_file_key_id(path, suffix):
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
    last_key, last_id = 0, 0
    files = list(path.glob(f'*{suffix}'))
    if len(files) == 0:
        return last_key, last_id
    for f in files[-1:]:
        last_key, last_id = name_2_dt_id(f.name, suffix)
    return last_key, last_id


def get_max_file_key_id(path, str_key='UPDATE_TIME', str_id='ID', suffix=FILE_SUFFIX):
    """得到目录下所有文件最大值

    Parameters
    ----------
    path
    str_key
    str_id
    suffix

    Notes
    -----
    不是分别最大值，而要进行复合排序

    """
    files = list(path.glob(f'*{suffix}'))
    if len(files) == 0:
        return 0, 0

    max_keys = []
    max_ids = []
    for f in files:
        df = pd.read_parquet(f)
        max_dt, max_id = max_key_id(df, str_key, str_id)
        max_keys.append(max_dt)
        max_ids.append(max_id)

    df = pd.DataFrame({
        str_key: max_keys,
        str_id: max_ids,
    })

    return max_key_id(df, str_key, str_id)


def get_files_count(path, suffix):
    """得到目录下的文件数量"""
    files = list(path.glob(f'*{suffix}'))
    return len(files)
