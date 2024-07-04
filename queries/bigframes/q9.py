from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes

import bigframes.pandas as pd

Q_NUM = 9


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    part_ds = utils.get_part_ds
    nation_ds = utils.get_nation_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()
    part_ds()
    nation_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal part_ds
        nonlocal nation_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        part_ds = part_ds()
        nation_ds = nation_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        psel = part_ds.p_name.str.contains("ghost")
        fpart = part_ds[psel]
        jn1 = line_item_ds.merge(fpart, left_on="l_partkey", right_on="p_partkey")
        jn2 = jn1.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        jn3 = jn2.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        jn4 = part_supp_ds.merge(
            jn3,
            left_on=["ps_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
        )
        jn5 = jn4.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        jn5["tmp"] = jn5.l_extendedprice * (1 - jn5.l_discount) - (
            (1 * jn5.ps_supplycost) * jn5.l_quantity
        )
        jn5["o_year"] = jn5.o_orderdate.dt.year
        gb = jn5.groupby(["n_name", "o_year"], as_index=False)["tmp"].sum()
        result_df = gb.sort_values(["n_name", "o_year"], ascending=[True, False])
        print(result_df.head(5))
        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
