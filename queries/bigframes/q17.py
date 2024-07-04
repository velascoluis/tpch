from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd

Q_NUM = 17


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    part_ds = utils.get_part_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    part_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal part_ds

        line_item_ds = line_item_ds()
        part_ds = part_ds()

        left = line_item_ds.loc[:, ["l_partkey", "l_quantity", "l_extendedprice"]]
        right = part_ds[
            ((part_ds["p_brand"] == "Brand#23") & (part_ds["p_container"] == "MED BOX"))
        ]
        right = right.loc[:, ["p_partkey"]]
        line_part_merge = left.merge(
            right, left_on="l_partkey", right_on="p_partkey", how="inner"
        )
        line_part_merge = line_part_merge.loc[
            :, ["l_quantity", "l_extendedprice", "p_partkey"]
        ]
        lineitem_filtered = line_item_ds.loc[:, ["l_partkey", "l_quantity"]]
        lineitem_avg = lineitem_filtered.groupby(
            ["l_partkey"], as_index=False
        ).agg(avg=pd.NamedAgg(column="l_quantity", aggfunc="mean"))
        lineitem_avg["avg"] = 0.2 * lineitem_avg["avg"]
        lineitem_avg = lineitem_avg.loc[:, ["l_partkey", "avg"]]
        total = line_part_merge.merge(
            lineitem_avg, left_on="p_partkey", right_on="l_partkey", how="inner"
        )
        total = total[total["l_quantity"] < total["avg"]]
        result_df = pd.DataFrame({"avg_yearly": [total["l_extendedprice"].sum() / 7.0]})
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
