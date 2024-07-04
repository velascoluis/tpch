from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
from datetime import date
import bigframes
import bigframes.pandas as pd

Q_NUM = 20


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    part_ds = utils.get_part_ds
    nation_ds = utils.get_nation_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    part_ds()
    nation_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal part_ds
        nonlocal nation_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        part_ds = part_ds()
        nation_ds = nation_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        date1 = date(1996, 1, 1)
        date2 = date(1997, 1, 1)
        psel = part_ds.p_name.str.startswith("azure")
        nsel = nation_ds.n_name == "JORDAN"
        lsel = (line_item_ds.l_shipdate >= date1) & (line_item_ds.l_shipdate < date2)
        fpart = part_ds[psel]
        fnation = nation_ds[nsel]
        flineitem = line_item_ds[lsel]
        jn1 = fpart.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        jn2 = jn1.merge(
            flineitem,
            left_on=["ps_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
        )
        gb = jn2.groupby(
            ["ps_partkey", "ps_suppkey", "ps_availqty"], as_index=False
        )["l_quantity"].sum()
        gbsel = gb.ps_availqty > (0.5 * gb.l_quantity)
        fgb = gb[gbsel]
        jn3 = fgb.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        jn4 = fnation.merge(jn3, left_on="n_nationkey", right_on="s_nationkey")
        jn4 = jn4.loc[:, ["s_name", "s_address"]]
        result_df = jn4.sort_values("s_name").drop_duplicates()
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
