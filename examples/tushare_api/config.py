import pathlib

# tushare令牌
TUSHARE_TOKEN = 'f50eb029b9b23a25f9298a9d6849a1127b8c5f1d3b468f9d3012ed72'

# tushare数据根目录。这里目前使用的是相对路径，请按自己的实际情况进行调整。
DATA_ROOT = pathlib.Path('../..').resolve() / 'data/tushare_api'

# 导入数据下载服务
import tushare as ts
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
