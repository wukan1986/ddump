"""
合并文件
1. 一组文件，第一个取前部分，最后一个取后部分，然后合并多个文件
2. 以其它方式命名的文件名，合并的方式需要另行处理

"""
import pandas as pd

from .common import start_end_2_name
from ..common import FILE_SUFFIX, START_SEP_END


def path_groupby_date(input_path, output_path,
                      reserve=2, suffix=FILE_SUFFIX):
    """根据文件名上所示时间进行分组

    文件名由 开始时间+结束时间组成

    Parameters
    ----------
    input_path: pathlib.Path
        输入目录
    output_path: pathlib.Path
        输出目录
    reserve: int
        预留文件数。最后几个文件不动，可能会被修改
    suffix

    Returns
    -------
    dict

    """
    if output_path is None:
        output_path = input_path

    files_tail = []
    files = list(input_path.glob(f'*{suffix}'))
    if reserve > 0:
        files_tail = files[-reserve:]
        files = files[:-reserve]

    # 提取文件名中的时间
    df = pd.DataFrame([f.name.split('.')[0].split(START_SEP_END) for f in files], columns=['start', 'end'])
    df['path'] = files
    df['key'] = pd.to_datetime(df['start'])
    df['key2'] = df['key']
    df.index = df['key'].copy()
    df.index.name = 'date'  # 防止无法groupby

    from dateutil.relativedelta import relativedelta, MO, SU
    from datetime import datetime, timedelta

    # 周week。少用，因为跨月跨年了，如果周重新切分成月年，会出错
    df['1W_1'] = df['key'].apply(lambda x: x.date() + relativedelta(weekday=MO(-1)))
    df['1W_2'] = df['key'].apply(lambda x: x.date() + relativedelta(weekday=SU(0)))

    # 月month
    df['1M_1'] = df['key'].apply(lambda x: x.date() + relativedelta(day=1))
    df['1M_2'] = df['key'].apply(lambda x: x.date() + relativedelta(day=31))
    # 季quarter
    df['1Q_1'] = df['key'].apply(lambda x: x.date() + relativedelta(month=((x.month - 1) // 3) * 3 + 1, day=1))
    df['1Q_2'] = df['key'].apply(lambda x: x.date() + relativedelta(month=((x.month - 1) // 3 + 1) * 3, day=31))
    # 年
    df['1Y_1'] = df['key'].apply(lambda x: x.date() + relativedelta(month=1, day=1))
    df['1Y_2'] = df['key'].apply(lambda x: x.date() + relativedelta(month=12, day=31))
    # 十年decade
    df['10Y_1'] = df['key'].apply(lambda x: x.date() + relativedelta(year=x.year // 10 * 10, month=1, day=1))
    df['10Y_2'] = df['key'].apply(lambda x: x.date() + relativedelta(year=x.year // 10 * 10 + 9, month=12, day=31))

    df['1M_1'] = pd.to_datetime(df['1M_1'])
    df['1Y_1'] = pd.to_datetime(df['1Y_1'])

    # 最近的两个月不动，两个月前的都按月合并
    t = f'{datetime.now() - timedelta(days=31 * 2):%Y-%m}'
    df['key'] = df.loc[:t, '1M_1']
    t = f'{datetime.now() - timedelta(days=365 * 1):%Y}'
    df['key'] = df.loc[:t, '1Y_1']
    df['key'].fillna(df['key2'], inplace=True)

    # 按key进行分组
    fss = {}
    for k, v in df.groupby(by='key'):
        name = start_end_2_name(v['start'][0], v['end'][-1])
        fss[output_path / f"{name}{FILE_SUFFIX}"] = v['path'].tolist()

    # 最近的两个文件不动
    fs = []
    for f in files_tail:
        fs.append(f)
        fss[output_path / f.name] = fs
        fs = []

    return fss
