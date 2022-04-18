# ddump
ddump(Data Dump)数据转存工具。主要解决以下问题：
1. 数据库的增量转存
2. API数据转存的通用模式
3. 数据的本地文件组织方案

本工具定位是数据下载，文件目录和文件名的组织方式以实现增量下载和减少下载量为首要目标，读取是否方便为次要目标。  
用户可能需要根据自己的使用习惯，将数据转存成其它格式，如导入到数据库等

## 为何使用文件存储，而不用数据库
1. 没有表结构的情况下，to_sql保存格式的效率很低，提前准备表结构又麻烦
2. 金融类数据特殊，并不需要随机访问。全量加载或按日期加载都是更常用的方法
3. 数据备份分享时，文件更方便

## 为何采用Parquet文件格式
1. csv格式，文本格式，读写慢，容易丢失精度
2. pickle格式，只能在Python下使用
3. HDF5格式，强大灵活，跨语言
4. parquet格式，列式存储，支持直接读取文件夹。跨语言，常用于大数据处理

## 安装
> pip install ddump -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade # 国内镜像下载

> pip install ddump -i https://pypi.org/simple --upgrade # 国外官方源下载

## 开发
> pip install -e .


## 数据库转存
请访问 [数据库转存文档](ddump/db/README.md)

## API转存
请访问 [API转存文档](ddump/api/README.md)

## 数据库工具
在开发本项目时，提炼了一个数据库ORM工具，它是对sqlalchemy的进一步封装，简单易用，可以直接映射已经存在的表。使用方法仿照聚宽的数据接口。
```python
from ddump.db.tool import DbTool

db = DbTool(url="mysql+pymysql://user:pasword@127.0.0.1:3306/tushare?charset=utf8")
db.show_tables()

db.describe('FDT_STK_AUDIT')

q = db.query(db.FDT_STK_AUDIT).limit(10)
df = db.run_query(q)
df

```

## 样例
参考 [examples](examples)，内有常见的几个库的调用示例，欢迎大家提供更多的案例
