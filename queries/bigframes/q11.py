from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes

import bigframes.pandas as pd

Q_NUM = 11


def q() -> None:

    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds
    nation_ds = utils.get_nation_ds

    # first call one time to cache in case we don't include the IO times
    part_supp_ds()
    supplier_ds()
    nation_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal part_supp_ds
        nonlocal supplier_ds
        nonlocal nation_ds

        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()
        nation_ds = nation_ds()

        partsupp_filtered = part_supp_ds.loc[:, ["ps_partkey", "ps_suppkey"]]
        partsupp_filtered["total_cost"] = (
            part_supp_ds["ps_supplycost"] * part_supp_ds["ps_availqty"]
        )
        supplier_filtered = supplier_ds.loc[:, ["s_suppkey", "s_nationkey"]]
        ps_supp_merge = partsupp_filtered.merge(
            supplier_filtered, left_on="ps_suppkey", right_on="s_suppkey", how="inner"
        )
        ps_supp_merge = ps_supp_merge.loc[
            :, ["ps_partkey", "s_nationkey", "total_cost"]
        ]
        nation_filtered = nation_ds[(nation_ds["n_name"] == "GERMANY")]
        nation_filtered = nation_filtered.loc[:, ["n_nationkey"]]
        ps_supp_n_merge = ps_supp_merge.merge(
            nation_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner"
        )
        ps_supp_n_merge = ps_supp_n_merge.loc[:, ["ps_partkey", "total_cost"]]
        sum_val = ps_supp_n_merge["total_cost"].sum() * 0.0001
        total = ps_supp_n_merge.groupby(["ps_partkey"], as_index=False).agg(
            value=pd.NamedAgg(column="total_cost", aggfunc="sum")
        )
        total = total[total["value"] > sum_val]
        result_df = total.sort_values("value", ascending=False)
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
