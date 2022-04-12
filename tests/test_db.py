import time

from ddump.db.tool import DbTool


def test_a():
    db = DbTool(url='sqlite:///sqlalchemy.db')
    # df = db.show_tables()
    df = db.read_sql('select * from customer')
    print(len(df))
    df['id'] += len(df)
    t0 = time.time()
    df.to_sql(df, 'customer')
    print(f"DbTool: Total time for {len(df)} records {time.time() - t0} sec")

# def test_b():
#     db = DbTool(url='sqlite:///sqlalchemy.db')
#     # df = db.show_tables()
#     df = db.read_sql('select * from customer')
#     q = db.query(db.customer)  # .filter(db.customer.id > 10000)
#     db.check_limit = -1
#     df = db.run_delete(q, auto_commit=True)
#     print(df)


def test_c():
    db = DbTool(url='sqlite:///sqlalchemy.db')
    # df = db.show_tables()
    df = db.read_sql('select * from customer')
    q = db.query(db.customer).filter(db.customer.id<1000)
    db.check_limit = -1
    df = db.run_query(q, coerce_float=True)
    print(len(df))


# def test_d():
#     db = DbTool(url='sqlite:///sqlalchemy.db')
#     objs = [db.customer(id=1 + i, name=f'{i}') for i in range(1000000)]
#     t0 = time.time()
#     db.insert(objs)
#     print(f"DbTool: Total time for 0 records {time.time() - t0} sec")
