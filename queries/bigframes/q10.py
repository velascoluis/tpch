from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
from datetime import date
import bigframes

import bigframes.pandas as pd

Q_NUM = 10


def q() -> None:

    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    customer_ds = utils.get_customer_ds
    nation_ds = utils.get_nation_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()
    customer_ds()
    nation_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal customer_ds
        nonlocal nation_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        customer_ds = customer_ds()
        nation_ds = nation_ds()

        date1 = date(1994, 11, 1)
        date2 = date(1995, 2, 1)

        osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
        lsel = line_item_ds.l_returnflag == "R"
        forders = orders_ds[osel]
        flineitem = line_item_ds[lsel]
        jn1 = flineitem.merge(forders, left_on="l_orderkey", right_on="o_orderkey")
        jn2 = jn1.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
        jn3 = jn2.merge(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        jn3["tmp"] = jn3.l_extendedprice * (1.0 - jn3.l_discount)
        gb = jn3.groupby(
            [
                "c_custkey",
                "c_name",
                "c_acctbal",
                "c_phone",
                "n_name",
                "c_address",
                "c_comment",
            ],
            as_index=False
        )["tmp"].sum()
        result_df = gb.sort_values("tmp", ascending=False)
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
