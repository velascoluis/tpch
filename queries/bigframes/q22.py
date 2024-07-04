from __future__ import annotations

from typing import TYPE_CHECKING

from queries.bigframes import utils
import bigframes
import bigframes.pandas as pd

Q_NUM = 22


def q() -> None:

    customer_ds = utils.get_customer_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    orders_ds()

    def query() -> bigframes.dataframe.DataFrame:
        nonlocal customer_ds
        nonlocal orders_ds

        customer_ds = customer_ds()
        orders_ds = orders_ds()

        customer_filtered = customer_ds.loc[:, ["c_acctbal", "c_custkey"]]
        customer_filtered["cntrycode"] = customer_ds["c_phone"].str.slice(0, 2)
        customer_filtered = customer_filtered[
            (customer_ds["c_acctbal"] > 0.00)
            & customer_filtered["cntrycode"].isin(
                ["13", "31", "23", "29", "30", "18", "17"]
            )
        ]
        avg_value = customer_filtered["c_acctbal"].mean()
        customer_filtered = customer_filtered[customer_filtered["c_acctbal"] > avg_value]
        # select only the keys that don't match by performing a left join and only selecting columns with an na value
        orders_filtered = orders_ds.loc[:, ["o_custkey"]].drop_duplicates()
        customer_keys = customer_filtered.loc[:, ["c_custkey"]].drop_duplicates()
        customer_selected = customer_keys.merge(
            orders_filtered, left_on="c_custkey", right_on="o_custkey", how="left"
        )
        customer_selected = customer_selected[customer_selected["o_custkey"].isna()]
        customer_selected = customer_selected.loc[:, ["c_custkey"]]
        customer_selected = customer_selected.merge(
            customer_filtered, on="c_custkey", how="inner"
        )
        customer_selected = customer_selected.loc[:, ["cntrycode", "c_acctbal"]]
        agg1 = customer_selected.groupby(
            ["cntrycode"], as_index=False
        ).size()
        agg1.columns = ["cntrycode", "numcust"]
        agg2 = customer_selected.groupby(["cntrycode"], as_index=False).agg(
            totacctbal=pd.NamedAgg(column="c_acctbal", aggfunc="sum")
        )
        total = agg1.merge(agg2, on="cntrycode", how="inner")
        result_df = total.sort_values(by=["cntrycode"], ascending=[True])
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
