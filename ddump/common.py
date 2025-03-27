#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-04-11
import hashlib
import pathlib
import pickle
import tempfile

from loguru import logger

FILE_SUFFIX = '.parquet'  # 文件后缀名
TEMP_SUFFIX = '.tmp'  # 临时文件后缀名
KEY_SEP_ID = '__'  # 文件名分隔符。注意:KEY中不要出现__否则无法区分。KEY中出现双下划线的概率比单下载线的概率低很多
START_SEP_END = '__'  # 文件名分隔符。用于区分开始时间与结束时间。
OLD_DATA_DAY = 86400 * 10  # N天前的老数据认为不再更新，不需要重复下载。最长放9天
TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'ddump'
TEMP_DIR.mkdir(parents=True, exist_ok=True)
logger.info(TEMP_DIR)


def get_key(func_name, args, kwargs, ext):
    # 将参数转换为字符串
    args_str = pickle.dumps(args)
    kwargs_str = pickle.dumps(kwargs)

    # 组合函数名和参数
    key_str = func_name.encode() + b':' + args_str + b':' + kwargs_str

    # 计算 MD5
    return TEMP_DIR / (hashlib.md5(key_str).hexdigest() + ext)


def write_obj(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)


def read_obj(path):
    try:
        with open(path, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None


def remove_obj(path):
    pathlib.Path(path).unlink(missing_ok=True)
