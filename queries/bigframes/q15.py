from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
from datetime import date
import bigframes.pandas as pd

Q_NUM = 15


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    supplier_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        supplier_ds = supplier_ds()

        lineitem_filtered = line_item_ds[
            (line_item_ds["l_shipdate"] >= date(1996, 1, 1))
            & (
                line_item_ds["l_shipdate"]
                < (date(1996, 4, 1))
            )
        ]
        lineitem_filtered["revenue_parts"] = lineitem_filtered["l_extendedprice"] * (
            1.0 - lineitem_filtered["l_discount"]
        )
        lineitem_filtered = lineitem_filtered.loc[:, ["l_suppkey", "revenue_parts"]]
        revenue_table = (
            lineitem_filtered.groupby("l_suppkey", as_index=False)
            .agg(total_revenue=pd.NamedAgg(column="revenue_parts", aggfunc="sum"))
            .rename(columns={"l_suppkey": "supplier_no"})
        )
        max_revenue = revenue_table["total_revenue"].max()
        revenue_table = revenue_table[revenue_table["total_revenue"] == max_revenue]
        supplier_filtered = supplier_ds.loc[
            :, ["s_suppkey", "s_name", "s_address", "s_phone"]
        ]
        total = supplier_filtered.merge(
            revenue_table, left_on="s_suppkey", right_on="supplier_no", how="inner"
        )
        result_df = total.loc[
            :, ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
        ]
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
