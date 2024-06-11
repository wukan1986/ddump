#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-02-14

"""
API接口数据下载工具

1. 不需要指定票池的下载。如：交易日历、停牌信息


"""

import pathlib

import pandas as pd
from loguru import logger

from .common import FILE_SUFFIX
from ..api.common import (
    files_to_dataframe,
    start_end_2_name,
    filter_range_in_dataframe,
    timeout_mtime)
from ..common import START_SEP_END


def func_pre_save(df, **kwargs):
    return df


def func_post_download(df, **kwargs):
    return df


class Dump:
    # 单文件路径
    file_path = None
    # 路径是否有日期
    path_is_date = False
    # 文件夹中的所有文件，用于判断文件是否已经存在
    files_df = None
    # 下载的单期数据
    dfs = []
    # 底层调用的函数名
    func_name = None
    # 位置参数
    args = ()
    # 命名参数
    kwargs = {}

    def __init__(self, api, path, file_names):
        """初始化

        Parameters
        ----------
        api:
            api接口
        path: str or pathlib.Path
            保存数据的目录
        file_names:list
            文件名组成字段。用于根据设置的参数生成文件名

        """
        self.api = api
        self.path = pathlib.Path(path)
        self.file_names = file_names
        self.reset()

    def reset(self):
        """重置"""
        self.dfs = []
        # self.file_path = None
        # self.files_df = None
        # self.func_name = None
        # self.args = ()
        # self.kwargs = {}

    def set_parameters(self, func_name, *args, **kwargs):
        """设置查询参数

        不同API不同，需要定制重载

        Parameters
        ----------
        func_name: str
        args: tuple
        kwargs: dict

        """
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs
        # 生成写入文件名
        try:
            filename = f'{START_SEP_END.join([self.kwargs[k] for k in self.file_names])}{FILE_SUFFIX}'
            # 处理路径中不能使用的字符
            filename = filename.translate(str.maketrans('\\/:*?"<>|', '_' * 9))
            self.file_path = self.path / filename
        except TypeError as e:
            self.file_path = None
        pass

    def exists(self, timeout):
        """检查文件是否存在，防止重复下载

        Parameters
        ----------
        timeout:int
            超时。秒。超时的文档当做不存在，需要下载

        Returns
        -------
        bool

        """
        if self.file_path is None:
            raise Exception('请在继承set_parameters时，设置self.file_path')

        if self.path_is_date:
            # 文件名中有时间，按结束时间处理
            self.files_df = files_to_dataframe(self.path)
            return False
        else:
            # 文件名中没有时间，按文件时间处理
            if self.file_path.exists():
                # 文件存在，但没有设置超时
                if timeout <= 0:
                    return True
                return not timeout_mtime(self.file_path, timeout)
            else:
                return False

    def download(self, kw, post_download=func_post_download, post_download_kwargs={}):
        """下载动作。每个API的函数与参数不同，需定制重载

        Returns
        -------
        pd.DataFrame
            查询的数据
        kw:
            需要传送的参数。None表示全传

        Notes
        -----
        1. 一定要将数据存于 `self.df` 中才能保存
        2. 无数据时最好返回 空DataFrame，不要返回None

        """
        logger.info('下载 {} {} {}', self.func_name, self.args, self.kwargs)
        api = getattr(self.api, self.func_name)

        # 只有约定的键才做为参数
        _kwargs = {k: v for k, v in self.kwargs.items() if k in kw}
        df = api(*self.args, **_kwargs)
        if df is not None:
            df = post_download(df, **post_download_kwargs)
            self.dfs.append(df)

        # logger.info('数据量 {} {} {} {}', len(self.df), self.func_name, self.args, self.kwargs)
        return self.dfs

    def save(self, pre_save=func_pre_save, pre_save_kwargs={}):
        """保存数据

        Parameters
        ----------
        pre_save: func
            保存前的处理函数，特殊处理用
        pre_save_kwargs: dict
            保存存前处理函数的参数

        """
        dfs = [pre_save(df, **pre_save_kwargs) for df in self.dfs]
        df = pd.concat(dfs)

        self.path.mkdir(parents=True, exist_ok=True)
        # 保存
        logger.info('保存 {} {} {}', len(df), self.func_name, self.file_path)
        try:
            df.to_parquet(self.file_path, compression='zstd')
            self.dfs = []
        except Exception as e:
            print(df)
            self.dfs = []
            raise e

    def load(self):
        """加载数据"""
        if self.file_path is None:
            raise Exception('请调用set_parameters, 帮助设置file_path')
        self.df = pd.read_parquet(self.file_path)
        return self.df


class Dump__start__end(Dump):
    path_is_date = True

    def __init__(self, api, path, start_name, end_name):
        """

        Parameters
        ----------
        api
        path
        start_name: str
            开始时间日期
        end_name: str
            结束时间日期

        """
        Dump.__init__(self, api, path, [start_name, end_name])
        self.start_name = start_name
        self.end_name = end_name

    def set_parameters(self, func_name, **kwargs):
        """设置参数

        Parameters
        ----------
        func_name

        """
        # 设置参数
        Dump.set_parameters(self, func_name, **kwargs)
        # 生成写入文件名
        start = pd.to_datetime(self.kwargs[self.start_name])
        end = pd.to_datetime(self.kwargs[self.end_name])
        self.file_path = self.path / f'{start_end_2_name(start, end)}{FILE_SUFFIX}'

    def exists(self, file_timeout, data_timeout):
        """文件是否存在。超时的文件当做不存在

        Parameters
        ----------
        file_timeout: int
            文件超时。小于此时间，认为文件已经存在，不用重复下载
        data_timeout: int
            数据超时。大于此时间，认为是老数据，没有必要重新下载

        """
        if Dump.exists(self, file_timeout):
            return True
        # 检查范围是否已经下载完
        start = pd.to_datetime(self.kwargs[self.start_name])
        end = pd.to_datetime(self.kwargs[self.end_name])
        df = filter_range_in_dataframe(self.files_df,
                                       start=start, end=end,
                                       file_timeout=file_timeout, data_timeout=data_timeout)

        return not df.empty


class Dump__date(Dump__start__end):
    def __init__(self, api, path, date_name):
        """一个日期的只是两个日期的特例

        Parameters
        ----------
        api
        path
        date_name: str
            日期字段名

        """
        Dump__start__end.__init__(self, api, path, date_name, date_name)
