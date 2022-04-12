import pathlib

import jqdatasdk as jq

JQ_USERNAME = '13912345678'
JQ_PASSWORD = '123456'
jq.auth(JQ_USERNAME, JQ_PASSWORD)

# tushare数据根目录。这里目前使用的是相对路径，请按自己的实际情况进行调整。
DATA_ROOT = pathlib.Path('../..').resolve() / 'data/jqdatasdk'
