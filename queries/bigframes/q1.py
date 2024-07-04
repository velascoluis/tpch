from __future__ import annotations

from datetime import date

import bigframes
import bigframes.pandas as pd

from queries.bigframes import utils

Q_NUM = 1


def q() -> None:
    line_item_ds = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal line_item_ds
        line_item_ds = line_item_ds()

        var1 = date(1998, 9, 2)

        filt = line_item_ds[line_item_ds["l_shipdate"] <= var1]

        # This is lenient towards pandas as normally an optimizer should decide
        # that this could be computed before the groupby aggregation.
        # Other implementations don't enjoy this benefit.
        filt["disc_price"] = filt.l_extendedprice * (1.0 - filt.l_discount)
        filt["charge"] = (
            filt.l_extendedprice * (1.0 - filt.l_discount) * (1.0 + filt.l_tax)
        )

        gb = filt.groupby(["l_returnflag", "l_linestatus"], as_index=False)
        agg = gb.agg(
            sum_qty=pd.NamedAgg("l_quantity", "sum"),
            sum_base_price=pd.NamedAgg("l_extendedprice", "sum"),
            sum_disc_price=pd.NamedAgg("disc_price", "sum"),
            sum_charge=pd.NamedAgg("charge", "sum"),
            avg_qty=pd.NamedAgg("l_quantity", "mean"),
            avg_price=pd.NamedAgg("l_extendedprice", "mean"),
            avg_disc=pd.NamedAgg("l_discount", "mean"),
            # Size not implemented on BQ Dataframes?
            # count_order=pd.NamedAgg("l_orderkey", "size"),
            count_order=pd.NamedAgg("l_orderkey", "count"),
        )

        result_df = agg.sort_values(["l_returnflag", "l_linestatus"])
        print(result_df.head(5))
        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
