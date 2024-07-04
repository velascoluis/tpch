from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd

Q_NUM = 21


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds
    nation_ds = utils.get_nation_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()
    supplier_ds()
    nation_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds
        nonlocal nation_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        supplier_ds = supplier_ds()
        nation_ds = nation_ds()

        lineitem_filtered = line_item_ds.loc[
            :, ["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"]
        ]

        # keep all rows that have another row in linetiem with the same orderkey and different suppkey
        lineitem_orderkeys = (
            lineitem_filtered.loc[:, ["l_orderkey", "l_suppkey"]]
            .groupby("l_orderkey", as_index=False)["l_suppkey"]
            .nunique()
        )
        lineitem_orderkeys.columns = ["l_orderkey", "nunique_col"]
        lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] > 1]
        lineitem_orderkeys = lineitem_orderkeys.loc[:, ["l_orderkey"]]

        # keep all rows that have l_receiptdate > l_commitdate
        lineitem_filtered = lineitem_filtered[
            lineitem_filtered["l_receiptdate"] > lineitem_filtered["l_commitdate"]
        ]
        lineitem_filtered = lineitem_filtered.loc[:, ["l_orderkey", "l_suppkey"]]

        # merge filter + exists
        lineitem_filtered = lineitem_filtered.merge(
            lineitem_orderkeys, on="l_orderkey", how="inner"
        )

        # not exists: check the exists condition isn't still satisfied on the output.
        lineitem_orderkeys = lineitem_filtered.groupby(
            "l_orderkey", as_index=False
        )["l_suppkey"].nunique()
        lineitem_orderkeys.columns = ["l_orderkey", "nunique_col"]
        lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] == 1]
        lineitem_orderkeys = lineitem_orderkeys.loc[:, ["l_orderkey"]]

        # merge filter + not exists
        lineitem_filtered = lineitem_filtered.merge(
            lineitem_orderkeys, on="l_orderkey", how="inner"
        )

        orders_filtered = orders_ds.loc[:, ["o_orderstatus", "o_orderkey"]]
        orders_filtered = orders_filtered[orders_filtered["o_orderstatus"] == "F"]
        orders_filtered = orders_filtered.loc[:, ["o_orderkey"]]
        total = lineitem_filtered.merge(
            orders_filtered, left_on="l_orderkey", right_on="o_orderkey", how="inner"
        )
        total = total.loc[:, ["l_suppkey"]]

        supplier_filtered = supplier_ds.loc[:, ["s_suppkey", "s_nationkey", "s_name"]]
        total = total.merge(
            supplier_filtered, left_on="l_suppkey", right_on="s_suppkey", how="inner"
        )
        total = total.loc[:, ["s_nationkey", "s_name"]]
        nation_filtered = nation_ds.loc[:, ["n_name", "n_nationkey"]]
        nation_filtered = nation_filtered[nation_filtered["n_name"] == "SAUDI ARABIA"]
        total = total.merge(
            nation_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner"
        )
        total = total.loc[:, ["s_name"]]
        total = total.groupby("s_name", as_index=False).size()
        total.columns = ["s_name", "numwait"]
        result_df = total.sort_values(by=["numwait", "s_name"], ascending=[False, True])
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
