#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author     :wukan
# @License    :(C) Copyright 2022
# @Date       :2022-04-11

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


class DbTool:
    """数据库工具

    连接数据库后，直接使用ORM或SQL进行操作

    Examples
    --------
    >>> tushare = DbTool(url="mysql+pymysql://user:pasword@127.0.0.1:3306/tushare?charset=utf8")
    >>> tushare.show_tables()

    >>> tushare.describe('FDT_STK_AUDIT')

    >>> q = tushare.query(tushare.FDT_STK_AUDIT).limit(10)
    >>> df = tushare.run_query(q)
    >>> df.columns = df.columns.str.lower()
    >>> df

    """

    def __init__(self, url, echo=False):
        """构造函数

        Parameters
        ----------
        url: str
            数据库访问串。如果密码有@等特殊符号，可能需要改成%40等
            参考：https://tool.chinaz.com/tools/urlencode.aspx
        echo: bool
            回显日志

        """
        engine = create_engine(url, echo=echo,
                               pool_recycle=3600, pool_pre_ping=True,
                               )  # fast_executemany=True
        self._engine = engine

        # 自动映射所有表
        Base = automap_base()
        Base.prepare(engine, reflect=True)
        self._tables = Base.classes.keys()

        # 会话
        Session = sessionmaker(bind=engine, autocommit=False)
        self.session = Session()

        # 注册查询语句
        self.query = self.session.query

        # 设置表名，希望不会出现表名冲突
        for key in Base.classes.keys():
            assert not hasattr(self, key), "表名与对象属性冲突"
            setattr(self, key, getattr(Base.classes, key))

        # 检查limit，默认需要检查，只给专业人士使用
        self.check_limit = 5000

    def show_tables(self):
        """显示所有表名

        Returns
        -------
        list

        """
        return self._tables

    def describe(self, table_name):
        """显示指定表结构

        Parameters
        ----------
        table_name: str
            表名

        Returns
        -------
        pd.DataFrame

        """
        insp = reflection.Inspector.from_engine(self._engine)
        columns = insp.get_columns(table_name)
        return pd.DataFrame(columns)

    def run_query(self, q):
        """查询结果

        Parameters
        ----------
        q: Query
            查询对象

        Returns
        -------
        pd.DataFrame

        """
        # 太长了
        logger.trace(str(q))

        if self.check_limit > 0:
            assert q._limit_clause is not None, '必须设置limit'
            assert q._limit_clause.value <= self.check_limit, f'limit不能大于{self.check_limit}'

        # self._engine.dispose()
        return pd.read_sql(q.statement, q.session.bind)

    def run_update(self, q, values, auto_commit=True):
        """更新数据，需要先查后更新

        Parameters
        ----------
        q: Query
            查询对象
        values: dict
            需要更新的数据
        auto_commit: bool
            是否自动提交

        Notes
        -----
        此处的q不能使用limit语句

        """
        count = q.update(values)

        if auto_commit:
            self.commit()

        return count

    def run_delete(self, q, auto_commit=True):
        """删除

        Parameters
        ----------
        q: Query
            查询对象
        auto_commit: bool
            是否自动提交

        Notes
        -----
        此处的q不能使用limit语句

        """
        count = q.delete()

        if auto_commit:
            self.commit()

        return count

    def insert(self, instances, auto_commit=True):
        """添加记录

        Parameters
        ----------
        instances:
            ORM对象或列表
        auto_commit: bool
            是否自动提交

        Examples
        --------
        >>> User = table.User
        >>> a = User(NAME='wukan', REMARK='aaaa')
        >>> b = User(NAME='aaaaa', REMARK='bbbb')
        >>> table.insert(a)
        >>> table.insert([a,b])

        References
        ----------
        https://docs.sqlalchemy.org/en/14/faq/performance.html

        """
        if isinstance(instances, list):
            # 使用批量插入
            self.session.bulk_save_objects(instances)
        else:
            self.session.add(instances)

        if auto_commit:
            self.commit()

    def commit(self):
        """对事务操作提交"""
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()

    def read_sql(self, sql, **kwargs):
        """通过sql语句查询

        Parameters
        ----------
        sql: str
            sql语句

        Returns
        -------
        pd.DataFrame

        """
        logger.info(sql)
        return pd.read_sql(sql, con=self._engine, **kwargs)

    def to_sql(self, df, name, if_exists='append', **kwargs):
        """将DataFrame插入到数据库

        TODO: 有时也很快, 需要再比较

        Parameters
        ----------
        df: pd.DataFrame
        name: str
        if_exists: str
        kwargs:

        """

        return df.to_sql(name, con=self._engine,
                         if_exists=if_exists, index=False, chunksize=10000,
                         **kwargs)


if __name__ == '__main__':
    # 显示全部数据，有点多，使用要小心
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 300)

    finance = DbTool(url="mysql://reader:0075AE8C303EB420@10.174.12.124/finance?charset=utf8")
    # 显示表名
    finance.show_tables()

    # 查某表
    q = finance.query(finance.CCTV_NEWS).limit(10)
    finance.run_query(q)

    # 显示表结构
    finance.describe('SW1_DAILY_PRICE')

    # 直接SQL查询
    finance.read_sql('select * from FUND_DIVIDEND limit 0,10')

    # 更新
    finance.run_update(q, {'A': 'B'})

    # 插入
    finance.insert(finance.CCTV_NEWS(title='abc'))
