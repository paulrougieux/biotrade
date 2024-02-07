""" Aggregate Comtrade at the 4 digit level


For example aggregate comtrade data from the 6 to the 4 digit level and store
the resulting aggregated data frame to a parquet file.

    >>> import biotrade
    >>> from biotrade.comtrade.aggregate import agg_4d
    >>> # Create a directory to save parquet files
    >>> comtrade_4d_dir = biotrade.data_dir / "comtrade" / "4d"
    >>> comtrade_4d_dir.mkdir(exist_ok=True)
    >>> # Load data in a loop, aggregate and save to parquet files
    >>> for this_code in ["44", "47", "48", "49"]:
    >>>     print(f"Preparing {this_code}")
    >>>     df = agg_4d(product_code_start=this_code)
    >>>     df.to_parquet(comtrade_4d_dir / f"ft{this_code}.parquet")

df.info() shows a memory usage of 733 MB
df_agg.info() shows a memory usage of 57 MB
The ratio of number of lines is only half: len(df_agg) / len(df) = 0.57.
The large difference in size is because we dropped many columns in the aggregation.

"""

from biotrade.comtrade import comtrade


def agg_4d(product_code_start, table=None):
    """ "Aggregate the input data frame to 4 digit level for all products starting with the given code.
    The table argument defaults to "yearly".
    """
    if table is None:
        table = "yearly"

    # Load Global trade data in a loop
    df = comtrade.db.select(
        table, product_code_start=product_code_start, period_start=2010
    )

    df["product_code_4d"] = df["product_code"].str[:4]
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code_4d",
        "period",
        "year",
        "unit",
        "flow_code",
        "flow",
    ]
    df_agg = df.groupby(index)[["net_weight", "quantity", "trade_value"]].agg("sum")

    del df
    return df_agg
