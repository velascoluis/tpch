from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd

Q_NUM = 16


def q() -> None:

    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    part_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal part_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        part_ds = part_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        part_filtered = part_ds[
            (part_ds["p_brand"] != "brand#45")
            & (~part_ds["p_type"].str.contains("^MEDIUM POLISHED"))
            & part_ds["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9])
        ]

        part_filtered = part_filtered.loc[
            :, ["p_partkey", "p_brand", "p_type", "p_size"]
        ]
        partsupp_filtered = part_supp_ds.loc[:, ["ps_partkey", "ps_suppkey"]]
        total = part_filtered.merge(
            partsupp_filtered, left_on="p_partkey", right_on="ps_partkey", how="inner"
        )
        total = total.loc[:, ["p_brand", "p_type", "p_size", "ps_suppkey"]]
        supplier_filtered = supplier_ds[
            supplier_ds["s_comment"].str.contains(r"customer(\s|\s)*complaints")
        ]
        supplier_filtered = supplier_filtered.loc[:, ["s_suppkey"]].drop_duplicates()
        # left merge to select only ps_suppkey values not in supplier_filtered
        total = total.merge(
            supplier_filtered, left_on="ps_suppkey", right_on="s_suppkey", how="left"
        )
        total = total[total["s_suppkey"].isna()]
        total = total.loc[:, ["p_brand", "p_type", "p_size", "ps_suppkey"]]
        result_df = total.groupby(
            ["p_brand", "p_type", "p_size"], as_index=False
        )["ps_suppkey"].nunique()
        total.columns = ["p_brand", "p_type", "p_size", "supplier_cnt"]
        total = total.sort_values(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            ascending=[False, True, True, True],
        )

        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
