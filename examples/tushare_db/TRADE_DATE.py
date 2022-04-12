from ddump.db.dump import continue_download3
from ddump.db.tool import DbTool
from examples.tushare_db.config import DATA_ROOT, TUSHARE_URL

if __name__ == '__main__':
    db = DbTool(url=TUSHARE_URL)

    for tbl_name, str_key1 in [
        ('MKT_STK_ADJFACTOR', 'TRADE_DATE'),
        ('MKT_STK_BASICINFO', 'TRADE_DATE'),
        ('MKT_STK_DPRICE', 'TRADE_DATE'),
        ('MKT_STK_SUSPEND_D', 'SUSPEND_DATE'),

        ('MKT_LIMIT_LIST', 'TRADE_DATE'),
        ('MKT_STK_MARGIN', 'TRADE_DATE'),

        ('MKT_STK_MONEYFLOW', 'TRADE_DATE'),
        ('MKT_STK_MONEYFLOW_HSGT', 'TRADE_DATE'),
        ('MKT_STK_MONEYFLOW_HSGT_TOP10_HS', 'TRADE_DATE'),

        ('MKT_STK_PRICELIMIT', 'TRADE_DATE'),

        ('MKT_IDX_DPRICE', 'TRADE_DATE'),
        ('MKT_IDX_BASICINFO', 'TRADE_DATE'),
        ('MKT_IDX_THS_DPRICE', 'TRADE_DATE'),

        ('TS_INDEX_WEIGHT', 'TRADE_DATE'),
    ]:
        continue_download3(db, tbl_name, DATA_ROOT,
                           str_key1=str_key1, str_id1='ID', str_dt1='UPDATE_TIME',
                           str_key2='UPDATE_TIME', str_id2='ID', str_dt2='UPDATE_TIME',
                           limit=20000,
                           merge=True)
