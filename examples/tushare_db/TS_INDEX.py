from ddump.db.dump import continue_download3
from ddump.db.tool import DbTool
from examples.tushare_db.config import DATA_ROOT, TUSHARE_URL

if __name__ == '__main__':
    db = DbTool(url=TUSHARE_URL)

    for tbl_name, str_key1 in [
        ('TS_INDEX', 'MARKET'),
    ]:
        continue_download3(db, tbl_name, DATA_ROOT,
                           str_key1=str_key1, str_id1='ID', str_dt1='UPDATE_TIME',
                           str_key2='UPDATE_TIME', str_id2='ID', str_dt2='UPDATE_TIME',
                           limit=20000,
                           merge=True)
