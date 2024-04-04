#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Get data frames containing both FAOSTAT and Comtrade data for sawnwood and
soybean related products. In these data frames, the Comtrade product and
country codes have been converted to their equivalent FAOSTAT codes.

    >>> from biotrade.common.compare import merge_faostat_comtrade
    >>> swd = merge_faostat_comtrade(faostat_table="forestry_trade",
    >>>                              comtrade_table="yearly",
    >>>                              faostat_code = [1632, 1633])
    >>> soy = merge_faostat_comtrade(faostat_table="crop_trade",
    >>>                              comtrade_table="yearly",
    >>>                              faostat_code = [236, 237, 238])

Select the same data from Comtrade and compare to check that the
merge_faostat_comtrade() method doesn't modify the Comtrade data.

    >>> from biotrade.common.products import comtrade_faostat_mapping
    >>> from biotrade.comtrade import comtrade
    >>> import numpy as np
    >>> faostat_product = [236, 237, 238]
    >>> code_map = comtrade_faostat_mapping.query("faostat_code.isin(@faostat_product)")
    >>> soy_comtrade = (
    >>>     comtrade.db.select("yearly", product_code = code_map["comtrade_code"])
    >>>     .merge(comtrade_faostat_mapping.rename(columns={"comtrade_code":"product_code"}),
    >>>            on="product_code")
    >>> )
    >>> index = ["year"]
    >>> index = ["year", "reporter", "partner"]
    >>> # TODO: make it work also for country codes, requires a merge with FAOSTAT codes
    >>> # index = ["year", "reporter_code", "partner_code"]
    >>> soy_agg = (soy
    >>>     .query("source =='comtrade' and element.str.contains('quantity')")
    >>>     .groupby(["product_code", "element"] + index)["value"].sum()
    >>>     .reset_index()
    >>>     .assign(flow = lambda x: x["element"].str.replace("_quantity",""))
    >>>     .rename(columns={"product_code":"faostat_code"})
    >>> )
    >>> soy_comtrade_agg = (
    >>>     soy_comtrade.groupby(["faostat_code", "flow"] + index)["net_weight"].sum()
    >>>     .reset_index()
    >>>     .merge(soy_agg, on=["faostat_code", "flow"] + index, how="outer", indicator=True)
    >>> )
    >>> np.testing.assert_allclose(soy_comtrade_agg.query("_merge=='both'")["net_weight"],
    >>>                            soy_comtrade_agg.query("_merge=='both'")["value"])
    >>> # Reporter and partner which are in the Comtrade data frame but not in
    >>> # the data returned by merge_faostat_comtrade()
    >>> soy_comtrade_agg.query("_merge=='left_only'")
    >>> soy_comtrade_agg.query("_merge=='right_only'")

Use monthly Comtrade data:

    >>> swd = merge_faostat_comtrade(faostat_table="forestry_trade",
    >>>                              comtrade_table="monthly",monthly
    >>>                              faostat_code = [1632, 1633])

"""

import warnings
import pandas
import numpy as np
import pandas as pd
import math
from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade.common.products import comtrade_faostat_mapping

ELEMENT_DICT = {
    "element_code": [5610, 5622, 5910, 5922],
    "element": ["import_quantity", "import_value", "export_quantity", "export_value"],
}


def replace_exclusively(df, code_column, code_dict, na_value=-1):
    """Replace codes with the dict and all other values with na_value

    :param series pandas series of product or country codes to replace
    :param dict dict of key value replacement pairs sent to the
        pandas.core.series.replace method.
    :return a panda series with replaced values

    Not available values (na_value) are represented as -1 by default, because it is
    easier to handle as grouping variable.

    """
    selector = df[code_column].isin(code_dict.keys())
    # Extract the corresponding names to get a nicer warning when available
    name_column = code_column.replace("_code", "")
    if name_column in df.columns:
        missing = df.loc[~selector, [code_column, name_column]].drop_duplicates()
    else:
        missing = df.loc[~selector, [code_column]].drop_duplicates().drop_duplicates()
    # If there are missing values display a warning and set the code to na_value
    if not missing.empty:
        warnings.warn(
            f"The following codes are present in {code_column} but missing "
            f"from the mapping dictionary:\n{missing}"
        )
        # Add missing keys to the dictionary and map them to the na_value
        code_dict = code_dict.copy()
        code_dict.update(dict(zip(missing[code_column], [na_value] * len(missing))))
    return df[code_column].replace(code_dict)


def transform_comtrade_using_faostat_codes(
    comtrade_table, faostat_code=None, comtrade_code=None, aggregate=True
):
    """Load and transform a Comtrade data table to use FAOSTAT product, country codes and names

    :param comtrade_table str: name of the comtrade table to select from
    :param list faostat_code: list of faostat codes to be loaded, default is None
    :param list comtrade_code: list of comtrade codes to be loaded, default is None
    :param boolean aggregate: data are aggregagted or not by product code, default is True
    into faostat codes, default is True

    The function makes Comtrade monthly data available with FAOSTAT codes. It
    also works on Comtrade yearly data.
    It does the following:

        1. Find the corresponding Comtrade codes using the mapping table
        2. Load Comtrade monthly data for the corresponding codes
        3. Replace comtrade country names by the FAOSTAT names
        4. Replace product codes and country codes by the FAOSTAT codes
        5. Reshape the Comtrade data to a longer format similar to FAOSTAT
        6. Aggregate Comtrade flows to the FAOSTAT product codes

    Example use:

        >>> from biotrade.common.compare import transform_comtrade_using_faostat_codes

    """
    # 1. Find the corresponding Comtrade codes using the mapping table
    if faostat_code:
        # Define the mapping codes of Faostat
        if comtrade_code is None:
            selector = comtrade_faostat_mapping["faostat_code"].isin(faostat_code)
            product_mapping = comtrade_faostat_mapping[selector]
            comtrade_code = product_mapping["comtrade_code"]
    elif comtrade_code is None:
        raise ValueError("You need to specify at least a faostat or comtrade code")
    # 2. Load Comtrade data for the corresponding codes
    df_wide = comtrade.db.select(comtrade_table, product_code=comtrade_code)
    # Replace Comtrade country codes by the FAOSTAT country codes
    country_mapping = faostat.country_groups.df[["faost_code", "un_code"]]
    country_dict = country_mapping.set_index("un_code").to_dict()["faost_code"]
    df_wide["reporter_code"] = replace_exclusively(
        df_wide, "reporter_code", country_dict
    )
    df_wide["partner_code"] = replace_exclusively(df_wide, "partner_code", country_dict)
    # Monthly and Yearly Comtrade data may slightly differ for country names. Use Faostat country names for consistency
    country_table = faostat.db.select("country")
    df_wide = df_wide.merge(
        country_table[["country_code", "country_name"]],
        how="left",
        left_on="reporter_code",
        right_on="country_code",
    )
    df_wide["reporter"] = df_wide["country_name"]
    df_wide.drop(columns=["country_code", "country_name"], inplace=True)
    df_wide = df_wide.merge(
        country_table[["country_code", "country_name"]],
        how="left",
        left_on="partner_code",
        right_on="country_code",
    )
    df_wide["partner"] = df_wide["country_name"]
    df_wide.drop(columns=["country_code", "country_name"], inplace=True)
    # Reshape Comtrade to long format
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "period",
        "year",
        "unit",
        "flag",
        "flow_code",
        "flow",
    ]
    df = df_wide.melt(
        id_vars=index,
        # We loose the quantity column, but it is not available
        # in the monthly data see comtrade/database.py
        value_vars=["net_weight", "trade_value"],
        var_name="element",
        value_name="value",
    )
    # Add units
    df["unit"] = df["element"].replace({"net_weight": "kg", "trade_value": "usd"})
    # A query of the Comtrade monthly data shows that the "quantity" column is always empty
    #     select * from raw_comtrade.monthly where quantity is not null limit 4;
    # Returns 0 rows
    # Rename element="net_weight" to "quantity" to be similar to FAOSTAT
    df["element"] = (
        df["flow"].str.lower().replace("-", "_", regex=True) + "_" + df["element"]
    )
    df["element"] = (
        df["element"]
        .replace("_trade", "", regex=True)
        .replace("_net_weight", "_quantity", regex=True)
    )
    # Aggregate
    if aggregate:
        # Replace Comtrade product codes by the FAOSTAT product codes
        product_dict = product_mapping.set_index("comtrade_code").to_dict()[
            "faostat_code"
        ]
        df["product_code"] = replace_exclusively(df, "product_code", product_dict)
        # The "flag" column is kept out of the index so lines with different flags can be aggregated
        index = [
            "reporter_code",
            "reporter",
            "partner_code",
            "partner",
            "product_code",
            "period",
            "year",
            "unit",
            "element",
        ]
        df_agg = df.groupby(index, dropna=False)["value"].agg("sum").reset_index()
        # Check that the Comtrade data didn't change after aggregation
        assert math.isclose(df.value.sum(), df_agg.value.sum())
        df = df_agg
    return df


def merge_faostat_comtrade(
    faostat_table,
    comtrade_table,
    faostat_code=None,
    comtrade_code=None,
    aggregate=True,
    strict=True,
):
    """Merge faostat and comtrade bilateral trade data.

    :param faostat_table str: name of the faostat table to select from
    :param comtrade_table str: name of the comtrade table to select from
    :param list faostat_code: list of faostat codes to be loaded
    :param list comtrade_code: list of comtrade codes to be loaded, default is None and in this case they are mapped into faostat codes
    :param boolean aggregate: data are aggregated or not by product code, default is True
    :param boolean strict: whether to raise an error or a warning on duplicated country

    The function does the following:

        1. Load FAOSTAT bilateral trade data for the given codes
        2. Load the transformed version of the Comtrade data with faostat codes
            using the method `transform_comtrade_using_faostat_codes`
        3. Aggregate Comtrade from monthly to yearly. For the last data point
            extrapolate to the current year based on values from the last 12 months
        4. Concatenate FAOSTAT and Comtrade data

    For example load palm oil data for both Faostat and Comtrade (yearly):

        >>> from biotrade.common.compare import merge_faostat_comtrade
        >>> palm_comp = merge_faostat_comtrade(faostat_table="crop_trade",
        >>>                                    comtrade_table="yearly",
        >>>                                    faostat_code = [257])

    For example load sawnwood data from both Faostat and Comtrade (monthly):

        >>> from biotrade.common.compare import merge_faostat_comtrade
        >>> swd = merge_faostat_comtrade(faostat_table="forestry_trade",
        >>>                              comtrade_table="monthly",
        >>>                              faostat_code = [1632, 1633])

    To investigate the number of periods reported for each country in the most
    recent years, I used:

        >>> df_comtrade = transform_comtrade_using_faostat_codes(
        >>>     comtrade_table="monthly", faostat_code = [1632, 1633])
        >>> (df_comtrade.query("year >= year.max() -2")
        >>>  .groupby(["reporter", "period"])["value"].agg("sum")
        >>>  .reset_index()
        >>>  .value_counts(["reporter"])
        >>>  .reset_index().to_csv("/tmp/value_counts.csv")
        >>> )

    Max reporting period:

        >>> (df_comtrade.groupby("reporter")["period"].max()
        >>>  .to_csv("/tmp/max_period.csv"))

    """
    # 1. Load FAOSTAT bilateral trade data for the given codes
    if faostat_code:
        df_faostat = faostat.db.select(faostat_table, product_code=faostat_code)
        product_names = df_faostat[["product_code", "product"]].drop_duplicates()
        element_df = pd.DataFrame.from_dict(ELEMENT_DICT)
        # Convert trade values from 1000 USD to USD
        codes = element_df[element_df["element"].str.endswith("_value")][
            "element_code"
        ].values.tolist()
        selector = df_faostat["element_code"].isin(codes)
        df_faostat.loc[selector, "value"] = df_faostat.loc[selector, "value"] * 1e3
        df_faostat.loc[selector, "unit"] = "usd"
        # Convert tonnes to kg
        codes = element_df[element_df["element"].str.endswith("_quantity")][
            "element_code"
        ].values.tolist()
        selector = df_faostat["element_code"].isin(codes)
        df_faostat.loc[selector, "value"] = df_faostat.loc[selector, "value"] * 1e3
        df_faostat.loc[selector, "unit"] = "kg"
    else:
        df_faostat = pandas.DataFrame(
            columns=[
                "reporter_code",
                "reporter",
                "partner_code",
                "partner",
                "product_code",
                "product",
                "element_code",
                "element",
                "period",
                "year",
                "unit",
                "value",
                "flag",
            ]
        )
    # 2. Load Comtrade bilateral trade data for the given codes
    df_comtrade = transform_comtrade_using_faostat_codes(
        comtrade_table=comtrade_table,
        faostat_code=faostat_code,
        comtrade_code=comtrade_code,
        aggregate=aggregate,
    )
    if comtrade_table == "monthly":
        # 3. Aggregate Comtrade from monthly to yearly. For the last data point
        #    extrapolate to the current year based on values from the last 12 months
        # Group by year and compute the sum of values for the 12 month in each year
        index = [
            "reporter_code",
            "reporter",
            "partner_code",
            "partner",
            "product_code",
            "year",
            "unit",
            "element",
        ]
        df_comtrade_agg = df_comtrade.groupby(index)["value"].agg("sum")
        # The last year is not necessarily complete and it might differ by
        # countries. For any country. Sum the values of the last 12 months instead.
        # We need to go back a bit further , because in March of 2022, there might
        # be advanced countries which reported January 2022, but other countries
        # which still have their last reporting period as June 2021, or even
        # further back in 2020.
        df_comtrade = df_comtrade.copy()  # .query("year >= year.max() - 3").copy()
        df_comtrade["max_period"] = df_comtrade.groupby("reporter")["period"].transform(
            max
        )
        df_comtrade["last_month"] = df_comtrade["max_period"] % 100
        df_comtrade["previous_year"] = df_comtrade["max_period"] // 100 - 1
        # For the special case of December, last year stays the same
        # last month is zero so that 0+1 becomes January
        is_december = df_comtrade["last_month"] == 12
        df_comtrade.loc[is_december, "previous_year"] = df_comtrade["max_period"] // 100
        df_comtrade.loc[is_december, "last_month"] = 0
        df_comtrade["max_minus_12"] = (
            df_comtrade["previous_year"] * 100 + df_comtrade["last_month"] + 1
        )
        df_recent = df_comtrade.query("period >= max_minus_12").copy()
        df_recent["year"] = df_recent["previous_year"] + 1
        df_recent_agg = df_recent.groupby(index)["value"].agg(value_est=sum)
        # Combine the aggregated yearly values with the estimate for the last year
        df = pandas.concat([df_comtrade_agg, df_recent_agg], axis=1).reset_index()
    else:
        # Add column value estimation with nan values since yearly data are not estimated and drop column period
        df = df_comtrade
        df["value_est"] = np.nan
        df.drop(columns="period", inplace=True)
    # Replace value by the estimate "value_est" where it is defined
    selector = ~df.value_est.isna()
    df.loc[selector, "value"] = df.loc[selector, "value_est"]
    df.drop(columns="value_est", inplace=True)
    # Add the column flag for the estimates
    df["flag"] = np.nan
    df.loc[selector, "flag"] = "estimate"
    df.loc[~selector, "flag"] = ""
    if comtrade_code is None:
        # Add FAOSTAT product names
        df = df.merge(product_names, on="product_code")
    # Add faostat element codes to comtrade data for consistency
    cols = ["element_code", "element"]
    element_code_faostat = pandas.DataFrame(columns=cols)
    db = faostat.db
    # Retrieve element codes of Faostat db
    for table_name in ["crop_trade", "forestry_trade"]:
        table = db.tables[table_name]
        element_code = db.read_sql_query(
            table.select()
            .distinct(table.c.element_code, table.c.element)
            .with_only_columns(table.c.element_code, table.c.element)
        )
        element_code_faostat = pandas.concat(
            [element_code_faostat, element_code], ignore_index=True
        )
    element_code_faostat = element_code_faostat.drop_duplicates().reset_index(drop=True)
    element_code_faostat = (
        element_code_faostat[
            element_code_faostat["element_code"].isin(ELEMENT_DICT["element_code"])
        ].sort_values(by="element_code")
    ).reset_index(drop=True)
    # Put as int element code
    element_code_faostat["element_code"] = element_code_faostat["element_code"].astype(
        "int64"
    )
    # Put a check on element codes
    if not element_code_faostat.equals(pd.DataFrame.from_dict(ELEMENT_DICT)):
        warnings.warn(
            f"Element types for trade have changed from\n{pd.DataFrame.from_dict(ELEMENT_DICT)}\nPlease check them"
        )
    # Merge on element
    df = df.merge(element_code_faostat, how="left", on=["element"])
    # Fill nan with -1 to let potential joins on element_code column too for comtrade data
    df["element_code"] = df["element_code"].fillna(-1).astype(int)
    # 4. Concatenate FAOSTAT and Comtrade data
    df_faostat.drop(columns="period", inplace=True)
    df_faostat["source"] = "faostat"
    df["source"] = "comtrade"
    df_concat = pandas.concat([df_faostat, df])
    df_concat = df_concat.reset_index(drop=True)
    # Place the "source" column first for readability
    cols = df_concat.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    df_concat = df_concat[cols]
    # Raise a warning if there are duplicates of country names associated to the same country code
    for col in ["reporter", "partner"]:
        country_code_unique = df_concat.drop_duplicates(subset=[col, col + "_code"])[
            [col, col + "_code"]
        ]
        duplicates = country_code_unique[
            country_code_unique.duplicated(subset=col + "_code", keep=False)
        ]
        if len(duplicates):
            msg = (
                "There is more than 1 country code match for a country name"
                + f"\n{duplicates.sort_values(col+'_code').values}\n"
                + "These country names are present in the FAOSTAT data:\n"
                + f"{set(duplicates[col]) & set(df_faostat[col].unique())}.\n"
                + "These country names are present in the Comtrade table "
                + "(using replacement names from the FAOSTAT country table):\n"
                + f"{set(duplicates[col]) & set(df[col].unique())}\n\n"
                + "To fix this issue:\n"
                + "1) Update the FAOSTAT country table with:\n"
                + "    >>> from biotrade.faostat import faostat\n"
                + "    >>> faostat.pump.update('country')\n"
                + "2) Update the FAOSTAT trade data with:\n"
                + "    >>> from biotrade.faostat import faostat\n"
                + f"    >>> faostat.pump.update(['{faostat_table}'])\n"
                + "3) If the issue persist after performing the above two points,\n"
                + "   The following file needs to be changed:\n"
                + "   biotrade/biotrade/config_data/faostat_country_groups.csv\n"
                + "   followed by\n"
                + "    >>> faostat.pump.update('country')\n"
            )
            if strict:
                msg += "\nUse the argument strict=False to ignore this error."
                raise ValueError(msg)
            if not strict:
                warnings.warn(msg)
    return df_concat
