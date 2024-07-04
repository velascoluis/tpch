from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd
import bigframes.pandas as pd

Q_NUM = 19


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

        Brand31 = "Brand#31"
        Brand43 = "Brand#43"
        SMBOX = "SM BOX"
        SMCASE = "SM CASE"
        SMPACK = "SM PACK"
        SMPKG = "SM PKG"
        MEDBAG = "MED BAG"
        MEDBOX = "MED BOX"
        MEDPACK = "MED PACK"
        MEDPKG = "MED PKG"
        LGBOX = "LG BOX"
        LGCASE = "LG CASE"
        LGPACK = "LG PACK"
        LGPKG = "LG PKG"
        DELIVERINPERSON = "DELIVER IN PERSON"
        AIR = "AIR"
        AIRREG = "AIRREG"
        lsel = (
            (
                ((line_item_ds.l_quantity <= 36) & (line_item_ds.l_quantity >= 26))
                | ((line_item_ds.l_quantity <= 25) & (line_item_ds.l_quantity >= 15))
                | ((line_item_ds.l_quantity <= 14) & (line_item_ds.l_quantity >= 4))
            )
            & (line_item_ds.l_shipinstruct == DELIVERINPERSON)
            & ((line_item_ds.l_shipmode == AIR) | (line_item_ds.l_shipmode == AIRREG))
        )
        psel = (part_ds.p_size >= 1) & (
            (
                (part_ds.p_size <= 5)
                & (part_ds.p_brand == Brand31)
                & (
                    (part_ds.p_container == SMBOX)
                    | (part_ds.p_container == SMCASE)
                    | (part_ds.p_container == SMPACK)
                    | (part_ds.p_container == SMPKG)
                )
            )
            | (
                (part_ds.p_size <= 10)
                & (part_ds.p_brand == Brand43)
                & (
                    (part_ds.p_container == MEDBAG)
                    | (part_ds.p_container == MEDBOX)
                    | (part_ds.p_container == MEDPACK)
                    | (part_ds.p_container == MEDPKG)
                )
            )
            | (
                (part_ds.p_size <= 15)
                & (part_ds.p_brand == Brand43)
                & (
                    (part_ds.p_container == LGBOX)
                    | (part_ds.p_container == LGCASE)
                    | (part_ds.p_container == LGPACK)
                    | (part_ds.p_container == LGPKG)
                )
            )
        )
        flineitem = line_item_ds[lsel]
        fpart = part_ds[psel]
        jn = flineitem.merge(fpart, left_on="l_partkey", right_on="p_partkey")
        jnsel = (
            (jn.p_brand == Brand31)
            & (
                (jn.p_container == SMBOX)
                | (jn.p_container == SMCASE)
                | (jn.p_container == SMPACK)
                | (jn.p_container == SMPKG)
            )
            & (jn.l_quantity >= 4)
            & (jn.l_quantity <= 14)
            & (jn.p_size <= 5)
            | (jn.p_brand == Brand43)
            & (
                (jn.p_container == MEDBAG)
                | (jn.p_container == MEDBOX)
                | (jn.p_container == MEDPACK)
                | (jn.p_container == MEDPKG)
            )
            & (jn.l_quantity >= 15)
            & (jn.l_quantity <= 25)
            & (jn.p_size <= 10)
            | (jn.p_brand == Brand43)
            & (
                (jn.p_container == LGBOX)
                | (jn.p_container == LGCASE)
                | (jn.p_container == LGPACK)
                | (jn.p_container == LGPKG)
            )
            & (jn.l_quantity >= 26)
            & (jn.l_quantity <= 36)
            & (jn.p_size <= 15)
        )
        jn = jn[jnsel]
        result_df = (jn.l_extendedprice * (1.0 - jn.l_discount)).sum()
        print(result_df.head(5))
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
