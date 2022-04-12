from ddump.db.dump import continue_download3
from ddump.db.tool import DbTool
from examples.tushare_db.config import DATA_ROOT, TUSHARE_URL

if __name__ == '__main__':
    db = DbTool(url=TUSHARE_URL)

    for tbl_name, str_key1 in [
        ('FDT_STK_AUDIT', 'END_DATE'),
        ('FDT_STK_BALANCESHEET', 'END_DATE'),
        ('FDT_STK_CASHFLOW', 'END_DATE'),
        ('FDT_STK_EXPRESS', 'END_DATE'),
        ('FDT_STK_FORECAST', 'END_DATE'),
        ('FDT_STK_INCOME', 'END_DATE'),
        ('FDT_STK_ISSUEDATE', 'END_DATE'),
        ('FDT_STK_MAINBZ', 'END_DATE'),
        ('TS_STK_DIV', 'END_DATE'),
        ('TS_STK_MANAGERHOLD', 'END_DATE'),
    ]:
        continue_download3(db, tbl_name, DATA_ROOT,
                           str_key1=str_key1, str_id1='ID', str_dt1='UPDATE_TIME',
                           str_key2='UPDATE_TIME', str_id2='ID', str_dt2='UPDATE_TIME',
                           limit=20000,
                           merge=True)
