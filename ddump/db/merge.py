#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-04-10

"""
文件合并相关操作

文件名是已经约定好的格式

时间__唯一ID.扩展名。 按文件大小合并
时间__时间.扩展名。 按时间合并

"""

import pathlib

import pandas as pd

from ..common import FILE_SUFFIX, KEY_SEP_ID


def path_groupby_size(input_path, output_path,
                      per_size=64 * 1024 * 1024,
                      reserve=2, suffix=FILE_SUFFIX):
    """合并目录，按文件大小分。超出后才切分。

    合并后的文件是最后一个文件的文件名。
    所有文件名都按字符串分隔离

    Parameters
    ----------
    input_path: pathlib.Path
        输入目录
    output_path: pathlib.Path
        输出目录
    per_size: int
        合并后每个文件大概大小。默认64MB
    reserve: int
        预留文件数。最后几个文件不动，可能会被修改，文件夹2一定要预留一定数量的文件
    suffix

    """
    if output_path is None:
        output_path = input_path

    files_tail = []
    files = list(input_path.glob(f'*{suffix}'))
    if reserve > 0:
        files_tail = files[-reserve:]
        files = files[:-reserve]

    fss = {}
    fs = []
    st_size = 0
    for f in files:
        st_size += f.stat().st_size
        fs.append(f)
        if st_size >= per_size:
            # 单文件满足大小
            fss[output_path / f.name] = fs
            fs = []
            st_size = 0

    # 最后一个需要补录
    if len(files) > 0:
        fss[output_path / f.name] = fs
        fs = []

    for f in files_tail:
        fs.append(f)
        fss[output_path / f.name] = fs
        fs = []

    return fss


def path_groupby_date(input_path, output_path,
                      reserve=2, suffix=FILE_SUFFIX):
    """根据日期进行合并

    文件由 key_id组合而成

    Parameters
    ----------
    input_path: pathlib.Path
        输入目录
    output_path: pathlib.Path
        输出目录
    reserve: int
        预留文件数。最后几个文件不动，因为可能会被修改，文件夹2一定要预留一定数量的文件
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
    df = pd.DataFrame([f.name.split('.')[0].split(KEY_SEP_ID) for f in files], columns=['key', 'id'])
    df['path'] = files
    df['key'] = pd.to_datetime(df['key'])
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
    fss = []
    for k, v in df.groupby(by='key'):
        # 1W_1是组内完全一样，所以直接取最后一行也行
        from_ = v['path'].tolist()
        to_ = output_path / from_[-1].name

        fss.append({'to': to_, 'from': from_})

    return fss
