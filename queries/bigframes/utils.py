from __future__ import annotations

from typing import TYPE_CHECKING, Any
import bigframes
import bigframes.pandas as pd

from queries.common_utils import (
    check_query_result_bigframes,
    get_table_path_bq,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable

settings = Settings()


def _read_ds(table_name: str) -> bigframes.dataframe.DataFrame:
    settings.paths = get_table_path_bq(table_name)
    return pd.read_gbq(settings.paths)


@on_second_call
def get_line_item_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("lineitem")


@on_second_call
def get_orders_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("orders")


@on_second_call
def get_customer_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("customer")


@on_second_call
def get_region_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("region")


@on_second_call
def get_nation_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("nation")


@on_second_call
def get_supplier_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("supplier")


@on_second_call
def get_part_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("part")


@on_second_call
def get_part_supp_ds() -> bigframes.dataframe.DataFrame:
    return _read_ds("partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(
        query, query_number, "bigframes", query_checker=check_query_result_bigframes
    )
