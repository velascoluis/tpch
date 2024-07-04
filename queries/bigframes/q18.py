from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd

Q_NUM = 18


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    customer_ds = utils.get_customer_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()
    customer_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal customer_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        customer_ds = customer_ds()

        gb1 = line_item_ds.groupby("l_orderkey", as_index=False)[
            "l_quantity"
        ].sum()
        fgb1 = gb1[gb1.l_quantity > 300]
        jn1 = fgb1.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        jn2 = jn1.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
        gb2 = jn2.groupby(
            ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"],
            as_index=False,
           
        )["l_quantity"].sum()
        result_df = gb2.sort_values(
            ["o_totalprice", "o_orderdate"], ascending=[False, True]
        )
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
