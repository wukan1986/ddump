#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-04-11

FILE_SUFFIX = '.parquet'  # 文件后缀名
TEMP_SUFFIX = '.tmp'  # 临时文件后缀名
KEY_SEP_ID = '__'  # 文件名分隔符。注意:KEY中不要出现__否则无法区分。KEY中出现双下划线的概率比单下载线的概率低很多
START_SEP_END = '__'  # 文件名分隔符。用于区分开始时间与结束时间。
OLD_DATA_DAY = 86400 * 10  # N天前的老数据认为不再更新，不需要重复下载。最长放9天
