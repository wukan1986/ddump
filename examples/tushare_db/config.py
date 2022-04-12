import pathlib

# tushare数据库 User@tushare123
TUSHARE_URL = 'mysql+pymysql://tushare:User%40tushare123@192.168.10.100:3306/tushare?charset=utf8'

# tushare数据根目录。这里目前使用的是相对路径，请按自己的实际情况进行调整。
DATA_ROOT = pathlib.Path('../..').resolve() / 'data/tushare_db'
