# 数据工具
1. 简易版数据库ORM工具，不需要提前编写数据表的结构代码。简单整合了常用的数据库功能
2. 提供了数据库增量同步的一种实现。防止同步进行中，有新数据修改时导致的数据丢失

## 使用方法
参考`examples/tushare_db`下的代码
1. 修改config.py中源数据库的地址账号密码
2. 按一定的要求调用`continue_download3`。主要是两组参数：
    1. 第一组参数，全量下单时所用到的`索引字段名`str_key1、`唯一ID字段名`str_id1、`修改时间字段名`str_dt1，此处的`索引字段名`用表中索引，可以加快转存速度
    2. 第二组参数，增量下单时所用到的`索引字段名`str_key2、`唯一ID字段名`str_id2、`修改时间字段名`str_dt2，此处`索引字段名`用的`修改时间字段名`一样的值
    3. 要转存的表名tbl_name、数据保存的根目录path，将在根目录下生成以表名加后缀的文件夹
    
```python
from ddump.db.dump import continue_download3
from ddump.db.tool import DbTool
from examples.tushare_db.config import DATA_ROOT, TUSHARE_URL

if __name__ == '__main__':
    db = DbTool(url=TUSHARE_URL)

    for tbl_name, str_key1 in [
        ('MKT_STK_DPRICE', 'TRADE_DATE'),
        ('MKT_STK_SUSPEND_D', 'SUSPEND_DATE'),

    ]:
        continue_download3(db, tbl_name, DATA_ROOT,
                           str_key1=str_key1, str_id1='ID', str_dt1='UPDATE_TIME',
                           str_key2='UPDATE_TIME', str_id2='ID', str_dt2='UPDATE_TIME',
                           limit=20000,
                           merge=True)

```

## 数据增量同步
1. 历史全量
    - 目的：解决数据量的快速下载
    - 方法：使用**索引字段**加快数据库搜索速度
    - 缺陷：此方法无法处理历史数据后期被更新的问题
2. 增量更新
    - 方法：使用**更新时间+唯一ID**保证数据不重复
    - 缺陷：在数据量大、更新时间无索引时比较慢
3. 删除
    - 缺陷：无法处理删除。除非改成标记删除，而不是真的删除

### 全量加增量可能出现的问题
全量下载耗时长。前段数据已经下完成，后段数据正在下

1. 前段数据更新，后段数据未更新。增量下载时不会出问题
2. 前段数据未更新，后段数据更新。增量下载时不会出问题
3. 前段数据更新，后段数据也更新。后段数据的更新时间导致前段数据丢失
 
### 机制设计迭代一，双文件夹模式
1. 文件名由 **排序KEY__唯一ID** 组成，将从文件名中提取增量参数
2. 同一文件夹下，排序KEY所采用字段必须统一，不能混用。所以全量与增量分别用两个文件夹
    1. `文件夹1`。历史全量。文件名由 **索引字段__唯一ID** 组成
    2. `文件夹2`。最新增量。文件名由 **更新时间__唯一ID** 组成。增量部分可能覆盖历史记录
3. 全量下载：开始时写入 **开始时间__0** 到文件夹2, 完成后写入 **开始时间__1** 到`文件夹2`。这是实现全量转增量特殊设计
4. 当`文件夹2`满足条件时，按`文件夹2`进行增量更新，反之按`文件夹1`更新
5. `文件夹2`下0个文件表示全量下载任务还没有开始，1个文件表示全量下载任务进行中，2个文件表示全量下载已经完成。

### 机制设计迭代二，三文件夹模式
1. 问题：部分数据源早期的数据更新时间字段值为空
2. 开始将所有 **更新时间** 为空的下载过来，保存放在`文件夹1`会有问题，因为文件夹内字段必须统一
3. 所以再建一个`文件夹0`，当`文件夹1`中没有文件时，按`文件夹0`下载。然后再按`文件夹1`下载
4. 之后 更新时间字段为空 的记录发生变更，定会 更新时间字段不为空，被记录到`文件夹2`，所以更新不存在问题

### 总结
1. 通过三个文件夹，以及文件夹中的文件数来进行下载过程控制
2. 文件夹2 > 1个。增量更新阶段
3. 文件夹2 = 1个，文件夹1 >= 0个。文件夹1正在全量下载中
4. 文件夹2 = 0个，文件夹1 = 0个，文件夹0 > 0个。文件夹0正在全量下载 更新时间为None的记录

### 其它
1. 由于写入文件需要时间，很有可能出现文件创建或写到一半程序崩溃了，所以先写入到临时文件再改名
2. 合并多个文件时也需要生成临时文件后再删除旧文件

### 目录示例
```bash
└─tushare_db
    ├─FDT_STK_AUDIT__0
    │      None__00000000000000048147.parquet
    ├─FDT_STK_AUDIT__1
    │      20211231__00000000000000073875.parquet
    ├─FDT_STK_AUDIT__2
    │      20220207T091615__00000000000000000001.parquet
    │      20220208T070018__00000000000000073885.parquet
    │      20220209T200021__00000000000000073893.parquet
    │      20220215T070017__00000000000000073908.parquet
    ├─FDT_STK_BALANCESHEET__1
    │      20211231__00000000000001142180.parquet
    ├─FDT_STK_BALANCESHEET__2
    │      20220207T091716__00000000000000000001.parquet
    │      20220209T070005__00000000000001147342.parquet
    │      20220210T070005__00000000000001147370.parquet
```