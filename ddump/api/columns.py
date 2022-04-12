"""
票池的维护

1. 成份股接口，按天查询，返回为list
2. 为减少流量，查询使用折半查找
3. 可能数据超量，前几次正常，后几次失败，所以需要每次查询后都存盘
4. 下载的东西可能有权重信息，也需要记录下来

对于权重更新频率为月，按天查询的情况，有些API返回了日期（聚宽），有些没有（wind）
有返回日期的可以加快定位

用户在下载对应数据时，最好能根据票池下载

"""
import pandas as pd

from loguru import logger


def next_query_dates(*args):
    """下一次的查询日期列表,以及本次的合并结果

    Parameters
    ----------
    args:
        多个查询结果列表

    Returns
    -------
    list
        下次查询的列表清单。
        - 未排除非交易日，有些接口没有数据可能有问题
        - 日期可能重复。所以
    pd.DataFrame
        合并排序后的数据

    """
    df = pd.concat(args)
    df = df[~df.index.duplicated(keep='last')]
    df = df.sort_index().fillna(0)
    d = (df == df.shift()).all(axis=1)
    d = pd.DataFrame(d, columns=['is_same'])
    d['curr'] = d.index
    d['prev'] = d['curr'].shift()
    if d.empty:
        return df, d, []
    # 1. 空数据时dt属性会失败
    # 2. 直接除2，没有排除节假日，在部分系统中节假日可能查出来为空
    d['half'] = pd.to_datetime((d['curr'] - (d['curr'] - d['prev']) // 2).dt.date)
    # 昨天，部分请求中昨天是一个关键点
    d['yestoday'] = d['curr'] - pd.Timedelta(days=1)
    #
    nx = d[~d['is_same']]['half'].dropna()
    # 选出出现不同的行，但不能用于折半，因为前后肯定不同，会导致再次查询
    # d[~d['is_same']]

    # 选出出现不同的行与其上一行，可用于折半查找，因为保留了边界
    # 这里不建议删除已经查出来的记录，因为可能事后有其他用途
    # d['is_same2'] = d['is_same'].shift(-1, fill_value=False)
    # d[~d['is_same']|~d['is_same2']]
    return df, d, nx.tolist()


def query_list_to_dataframe(func, date, symbol):
    """将查询的列表转换成DataFrame

    Parameters
    ----------
    func:
        查询函数
    symbol
        查询参数
    date
        查询日期

    Returns
    -------
    pd.DataFrame
        日期为索引的DataFrame。查询日期与返回日期可能不同，需留意
    list
        推荐的下次查询日期

    """
    date = pd.to_datetime(date)
    columns = func(symbol, date=date)
    # 如果遇到查的是A日，返回的是B日，还没办法处理
    df = pd.DataFrame(index=[date], columns=columns)
    df[:] = 1.0
    return df, []


def query_dataframe_to_dateframe(func, date, symbol):
    """查出来的DataFrame转成特殊的DataFrame，每个函数不同，需定制

    日期使用传回的日期，可能导致折半查找进入死循环，所以将两个一起使用
    这里要注意的是，查询日期不能超前，否则错误数据会被记录
    """
    date = pd.to_datetime(date)
    df = func(symbol, date=date)
    df['date'] = pd.to_datetime(df['date'])  # 格式处理需处理
    df.set_index('date', append=True, inplace=True)
    df = df['weight'].unstack(level=0)
    # 推荐下次的结果
    nx = df.index - pd.Timedelta(days=1)
    # 伪造查询日期时的权重
    df = pd.concat([df, pd.DataFrame.from_records(df.tail(1), index=[date])])
    # 日期可能重复，保留第一个，因为最后个是伪造的
    # 多次查询后需要合并，这时就得保留最新的一个
    df = df[~df.index.duplicated(keep='first')]
    return df, nx.tolist()


def binsearch_download(query_func,
                       load_func,
                       save_func,
                       *args):
    """折半下载

    Parameters
    ----------
    query_func
    load_func
    save_func
    args

    Returns
    -------
    pd.DataFrame

    """

    def difference(a, b):
        """集合差集"""
        return list(set(a) - set(b))

    # 输入的数据
    try:
        df = load_func()
    except FileNotFoundError as e:
        df = pd.DataFrame()

    # 将字符串时间转换成时间
    args = pd.to_datetime(pd.Series(args)).tolist()
    new_dfs = []
    recommends = []

    while True:
        df, d, n = next_query_dates(df, *new_dfs)
        new_dfs = []  # 用完要立即清理
        # df 需要保存一下，否则可能出现异常丢失
        # 如果不能保存，立即中断不用再查了
        logger.info('开始保存，数量 {}', len(df))
        save_func(df)

        n.extend(args)

        # 合并推荐的下次查询参数
        for r in recommends:
            # 只有在范围内的才推荐
            if df.index[0] < r < df.index[-1]:
                n.append(r)
        recommends = []

        n = difference(n, df.index)

        # 没有新日期要查询，返回
        if len(n) == 0:
            break

        logger.info('开始查询: {}', n)
        try:

            for i in n:
                result, recommend = query_func(i)
                new_dfs.append(result)
                recommends.extend(recommend)
        except Exception as e:
            logger.warning(repr(e))

        # 查询结果为空返回，有可能是上一步出错了，必须要跳出循环
        if len(new_dfs) == 0:
            break

    return df
