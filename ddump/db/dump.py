#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-04-10

"""
如何将整张表下载过来？

1. 表可能很大，无法一次性下载，需要分页下载，例如100GB
2. API上有限制，下载数据有量限制，例如一次只能下1000条
3. 表的字段有限。 有ID和update_time已经是最佳方案
3. update_time大量相同，有可能全表值都完全一样，导致每次都从头下载

References
----------
https://blog.csdn.net/oro99/article/details/120041289

```
where (update_time='last_..' and id>'...' ) or update_time>'last_...'  order by update_time asc, id asc
```

"""
import pathlib

import pandas as pd
from loguru import logger
from sqlalchemy import or_, and_

from .common import last_key_id, key_id_2_name, get_last_file_key_id, get_files_count
from ..common import FILE_SUFFIX, TEMP_SUFFIX
from .merge import path_groupby_size, path_groupby_date
from ..merge import merge_files_dict


def dump_table(db, tbl, q=None,
               str_key='UPDATE_TIME', str_id='ID', str_dt='UPDATE_TIME',
               last_key=0, last_id=0, last_dt=0,
               limit=5000,
               page=5,
               eq_none=False):
    """分页下载整表

    Parameters
    ----------
    db: DbTool
        自研数据库工具
    tbl: str or Table
        数据库表
    q: sqlalchemy.orm.query.Query
        查询条件
    str_key: str
        索引字段名
    str_id: str
        唯一ID字段名
    str_dt: str
        更新时间字段
    last_key:
        上次最新字段
    last_id: int
        上次最新ID
    last_dt: datetime
        上次最新时间
    limit: int
        每页数量
    page: int
        分页几张
    eq_none: bool
        是否查条件为None

    Returns
    -------
    df
    last_key
    last_id

    """
    # 类型提前调整
    if isinstance(tbl, str):
        tbl = getattr(db, tbl)
    c_key = getattr(tbl, str_key)
    c_id = getattr(tbl, str_id)
    c_dt = getattr(tbl, str_dt)

    # 没有设置查询条件就全查
    if q is None:
        q = db.query(tbl)

    dfs = []
    # 底层做几次增量查询，查看效果如何
    for i in range(page):
        if eq_none:
            # 早期，部分数据可能为空，所以直接全查
            _q = q.filter(c_key == None, c_id > last_id).order_by(c_id).limit(limit)
        else:
            # 先按时间排序，再按ID排序，还要过滤当前时间
            _q = q.filter(
                or_(
                    and_(c_key == last_key, c_id > last_id),
                    c_key > last_key
                )
            ).filter(
                c_dt < last_dt
            ).order_by(c_key, c_id).limit(limit)
        logger.info('过滤条件 key:{} id:{} dt:{}', last_key, last_id, last_dt)
        db.check_limit = -1
        df = db.run_query(_q)
        if df.empty:
            break
        dfs.append(df)
        # 更新关键参数
        last_key, last_id = last_key_id(df, str_key, str_id)
        if len(df) < limit:
            # 已经掏空
            break

    # 将多次查询的结果合并
    if len(dfs) > 0:
        df = pd.concat(dfs, ignore_index=True)

    return df, last_key, last_id


def continue_download(db, tbl_name, path,
                      str_key='UPDATE_TIME', str_id='ID', str_dt='UPDATE_TIME',
                      last_dt=None,
                      limit=5000,
                      eq_none=False):
    """增量下载, 从最后一个文件之后进行下载

    依据 更新时间+唯一ID

    Parameters
    ----------
    db: DbTool
        数据库工具
    tbl_name: str
        表名
    path: pathlib.Path
        保存路径
    str_key: str
        索引字段名
    str_id: str
        唯一ID字段名,同索引时实现增量查询
    str_dt: str
        更新时间字段名
    last_dt: datetime like
        开启全量下载的时间，同时也是全量下载的截止时间
    limit: int
        每次查询的数量限制
    eq_none: bool
        更新时间是否为None

    Returns
    -------
    bool
        是否还有后续

    """
    logger.info('下载 {}', tbl_name)
    page = 5  # 页数

    # 读取目录中 数据内的 两个关键参数
    last_key, last_id = get_last_file_key_id(path, FILE_SUFFIX)

    # 查数据库，得到最新的两个参数
    df, last_key, last_id = dump_table(db, tbl_name, q=None,
                                       str_key=str_key, str_id=str_id, str_dt=str_dt,
                                       last_key=last_key, last_id=last_id, last_dt=last_dt,
                                       limit=limit,
                                       page=page,
                                       eq_none=eq_none)
    if df.empty:
        logger.info('新结果为空，下载 {} 完成', tbl_name)
        return False

    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f'{key_id_2_name(last_key, last_id)}{FILE_SUFFIX}'
    temp_path = path / f'{key_id_2_name(last_key, last_id)}{TEMP_SUFFIX}'

    logger.info('保存 {}条 {} {}', len(df), tbl_name, temp_path)
    # 写入要时间，所以先放临时文件，然后再改名
    df.to_parquet(temp_path, compression='zstd')
    temp_path.rename(file_path)
    if len(df) < limit * page:
        # 下载完成
        return False
    return True


def continue_download3(db, tbl_name,
                       path,
                       str_key1='END_DATE', str_id1='ID', str_dt1='UPDATE_TIME',
                       str_key2='UPDATE_TIME', str_id2='ID', str_dt2='UPDATE_TIME',
                       limit=5000,
                       merge=True):
    """3个文件夹自动全量增量下载

    文件夹0: 更新时间为空
    文件夹1: 带索引的快速大数据下载
    文件夹2: 增量下载

    Parameters
    ----------
    db
    tbl_name
    path
    str_key1, str_id1, str_dt1
        全量查询时的字段
    str_key2, str_id2, str_dt2
        增量查询时的字段
    limit: int
        数据查询limit参数，一次只查多少行。全量下载数据量过大，只能分批次下载
    merge: bool
        是否合并多个文件

    """
    from datetime import datetime

    path0 = path / f'{tbl_name}__0'  # 更新时间为None数据
    path1 = path / f'{tbl_name}__1'  # 全量下载数据
    path2 = path / f'{tbl_name}__2'  # 增量下载数据

    count0 = get_files_count(path0, FILE_SUFFIX)
    count1 = get_files_count(path1, FILE_SUFFIX)
    count2 = get_files_count(path2, FILE_SUFFIX)
    logger.info('根文件夹:{}/{}_[012], 文件数量:{}/{}/{}', path, tbl_name, count0, count1, count2)

    if count2 == 0:
        # 文件夹2为空，表示全新。需要全量下载
        if count1 == 0:
            # 文件夹1为空，表示需要先下载 更新时间为空的部分
            last_dt, last_id = get_last_file_key_id(path0, FILE_SUFFIX)
            logger.info('{} 开始 {}==None 部分下载', tbl_name, str_dt2)
            while continue_download(db, tbl_name, path0,
                                    str_key=str_key2, str_id=str_id2, str_dt=str_dt2, last_dt=last_dt,
                                    limit=limit, eq_none=True):
                pass

        # 第一次写入文件，表示开始启动下载的时间
        # 如果文件夹1中已经有一部分数据了，已经下载的部分将没有机会更新了，这里需要注意
        path2.mkdir(parents=True, exist_ok=True)

        now = datetime.now()
        file_path = path2 / f'{key_id_2_name(now, 0)}{FILE_SUFFIX}'
        pd.DataFrame().to_parquet(file_path)

        logger.info('{} 全量 开始下载', tbl_name)
        while continue_download(db, tbl_name, path1,
                                str_key=str_key1, str_id=str_id1, str_dt=str_dt1, last_dt=now,
                                limit=limit, eq_none=False):
            pass

    count2 = get_files_count(path2, FILE_SUFFIX)
    if count2 == 1:
        # 文件夹2中只有一个文件，表示是文件夹1中还没下完，从文件名提取时间后继续下载
        last_dt, last_id = get_last_file_key_id(path2, FILE_SUFFIX)
        logger.info('{} 全量 继续下载', tbl_name)
        while continue_download(db, tbl_name, path1,
                                str_key=str_key1, str_id=str_id1, str_dt=str_dt1, last_dt=last_dt,
                                limit=limit, eq_none=False):
            pass

        # 第二次写入文件，表示已经下载完成，时间接着上次，编号+1，这样才会继续
        file_path = path2 / f'{key_id_2_name(last_dt, 1)}{FILE_SUFFIX}'
        pd.DataFrame().to_parquet(file_path)
    count2 = get_files_count(path2, FILE_SUFFIX)
    if count2 >= 2:
        now = datetime.now()
        logger.info('{} 增量 继续下载', tbl_name)
        while continue_download(db, tbl_name, path2,
                                str_key=str_key2, str_id=str_id2, last_dt=now,
                                limit=limit, eq_none=False):
            pass

    if merge:
        # 第一次合并压缩
        merge3(path0, path1, path2)
        # 第一次合并压缩，可能还满足合并条件，所以做第二次，防止下次又合并
        merge3(path0, path1, path2)


def merge3(path0, path1, path2):
    """三路径合并文件夹操作

    Parameters
    ----------
    path0: pathlib.Path
        更新时间为空的文件夹
    path1: pathlib.Path
        全量下载文件夹
    path2: pathlib.Path
        增量更新文件夹

    """
    # 合并历史数据，方便复制
    files = path_groupby_size(path0, path0, per_size=128 * 1024 * 1024, reserve=0)
    merge_files_dict(files, ignore_index=True, delete_src=True)
    files = path_groupby_size(path1, path1, per_size=128 * 1024 * 1024, reserve=0)
    merge_files_dict(files, ignore_index=True, delete_src=True)
    # 每日更新部分，不能老合并，因为修改太多，网络传输量会过大，改成两个月前的都合成一个月，最近两个月的不动
    files = path_groupby_date(path2, path2, reserve=2)
    merge_files_dict(files, ignore_index=True, delete_src=True)
