import pathlib

# tushare数据根目录。这里目前使用的是相对路径，请按自己的实际情况进行调整。
DATA_ROOT = pathlib.Path('../..').resolve() / 'data/akshare'
DATA_ROOT = pathlib.Path(r'D:\data\akshare')

# 导入数据下载服务
import akshare as ak

