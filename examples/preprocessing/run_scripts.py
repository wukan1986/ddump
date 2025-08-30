from examples import jqresearch as jqr  # noqa
from examples import preprocessing as prp  # noqa

prp.rename_last.main()

jqr.get_index_weights.main()
jqr.get_all_securities.main()
jqr.get_fundamentals_valuation.main()
jqr.get_fundamentals_season.main()
jqr.get_price_stock_daily.main()
jqr.get_price_stock_minute.main()
jqr.get_price_index_daily.main()
jqr.get_price_index_minute.main()
jqr.get_price_futures_daily.main()
jqr.get_price_futures_minute.main()

prp.merge_jqresearch.main()
prp.remove_tmp.main()

prp.step1.main()
prp.step2.main()
prp.step3.main()
