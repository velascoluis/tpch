from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes

import bigframes.pandas as pd

Q_NUM = 13


def q() -> None:

    customer_ds = utils.get_customer_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    orders_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal customer_ds
        nonlocal orders_ds

        customer_ds = customer_ds()
        orders_ds = orders_ds()

        customer_filtered = customer_ds.loc[:, ["c_custkey"]]
        orders_filtered = orders_ds[
            ~orders_ds["o_comment"].str.contains(r"special[\s|\s]*requests")
        ]
        orders_filtered = orders_filtered.loc[:, ["o_orderkey", "o_custkey"]]
        c_o_merged = customer_filtered.merge(
            orders_filtered, left_on="c_custkey", right_on="o_custkey", how="left"
        )
        c_o_merged = c_o_merged.loc[:, ["c_custkey", "o_orderkey"]]
        count_df = c_o_merged.groupby(["c_custkey"], as_index=False).agg(
            c_count=pd.NamedAgg(column="o_orderkey", aggfunc="count")
        )
        total = count_df.groupby(["c_count"], as_index=False).size()
        total.columns = ["c_count", "custdist"]
        result_df = total.sort_values(
            by=["custdist", "c_count"], ascending=[False, False]
        )
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
