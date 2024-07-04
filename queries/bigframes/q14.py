from __future__ import annotations

from typing import TYPE_CHECKING
from datetime import date
from queries.bigframes import utils
import bigframes

import bigframes.pandas as pd

Q_NUM = 14


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

        startdate = date(1994, 3, 1)
        enddate = date(1994, 4, 1)
        p_type_like = "PROMO"

        part_filtered = part_ds.loc[:, ["p_partkey", "p_type"]]
        lineitem_filtered = line_item_ds.loc[
            :, ["l_extendedprice", "l_discount", "l_shipdate", "l_partkey"]
        ]
        sel = (lineitem_filtered.l_shipdate >= startdate) & (
            lineitem_filtered.l_shipdate < enddate
        )
        flineitem = lineitem_filtered[sel]
        jn = flineitem.merge(part_filtered, left_on="l_partkey", right_on="p_partkey")
        jn["tmp"] = jn.l_extendedprice * (1.0 - jn.l_discount)
        result_df = (
            jn[jn.p_type.str.startswith(p_type_like)].tmp.sum() * 100 / jn.tmp.sum()
        )
        print(result_df)
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
