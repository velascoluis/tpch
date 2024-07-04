from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import date
from queries.bigframes import utils
import bigframes

import bigframes.pandas as pd

Q_NUM = 12


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        date1 = date(1994, 1, 1)
        date2 = date(1995, 1, 1)

        sel = (
            (line_item_ds.l_receiptdate < date2)
            & (line_item_ds.l_commitdate < date2)
            & (line_item_ds.l_shipdate < date2)
            & (line_item_ds.l_shipdate < line_item_ds.l_commitdate)
            & (line_item_ds.l_commitdate < line_item_ds.l_receiptdate)
            & (line_item_ds.l_receiptdate >= date1)
            & (
                (line_item_ds.l_shipmode == "mail")
                | (line_item_ds.l_shipmode == "ship")
            )
        )
        flineitem = line_item_ds[sel]
        jn = flineitem.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")

        def g1(x):
            return ((x == "1-URGENT") | (x == "2-HIGH")).sum()

        def g2(x):
            return ((x != "1-URGENT") & (x != "2-HIGH")).sum()

        total = jn.groupby("l_shipmode", as_index=False)["o_orderpriority"].agg((g1, g2))
        total = total.reset_index()  # reset index to keep consistency with pandas
        # skip sort when groupby does sort already
        result_df = total.sort_values("l_shipmode")
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
