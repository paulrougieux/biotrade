"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script which contains functions used to produce data for the Web Platform

"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from biotrade.faostat import faostat
from biotrade import data_dir
from biotrade.common.compare import merge_faostat_comtrade
from biotrade.common.time_series import (
    relative_absolute_change,
    segmented_regression,
    merge_analysis,
)

# Define column name suffixes for average, total and percentage calculations
COLUMN_AVG_SUFFIX = "_avg_value"
COLUMN_PERC_SUFFIX = "_percentage"
COLUMN_TOT_SUFFIX = "_tot_value"


def replace_zero_with_nan_values(df, column_list):
    """
    Replace zero value columns with nan, to let the values be dropped afterwards

    :param df (DataFrame), output to be saved
    :param column_list (list), name of columns to replace 0 with nan values
    :return df (DataFrame), with nan replacements

    """
    df[column_list] = df[column_list].replace(0, np.nan)
    return df


def save_file(df, file_name):
    """
    Function which save output scripts

    :param df (DataFrame), output to be saved
    :param file_name (str), name of the output file

    """
    # Save csv files to env variable path or into biotrade data folder alternatively
    if os.environ.get("FRONT_END_DATA"):
        path = Path(os.environ["FRONT_END_DATA"])
    else:
        path = data_dir / "front_end"
    path.mkdir(exist_ok=True)
    df.to_csv(path / file_name, index=False, na_rep="null")
    print(f"Saved file {file_name} to {path}")


def comtrade_products():
    """
    Return the regulation product codes and names together with the associated 6 digit codes of Comtrade products and Faostat contained into the file biotade/config_data/regulation_products.csv

    :return df (Dataframe), containing Comtrade and Faostat product codes and names
    """
    # Name of product file to retrieve
    main_product_file = faostat.config_data_dir / "regulation_products.csv"
    # Retrieve dataset
    df = pd.read_csv(
        main_product_file,
        dtype={"regulation_code": str, "hs_4d_code": str, "hs_6d_code": str},
    )
    # Retrieve name and codes and return the dataframe
    columns = [
        "regulation_code",
        "regulation_short_name",
        "commodity_short_name",
        "hs_6d_code",
        "fao_code",
        "match",
    ]
    df = df[columns]
    df.rename(
        columns={
            "regulation_code": "product_code",
            "regulation_short_name": "product_name",
            "commodity_short_name": "commodity_name",
            "hs_6d_code": "comtrade_code",
        },
        inplace=True,
    )
    return df


def main_product_list(table_list):
    """
    Return the main list of Faostat products (without duplicates) contained into the file biotade/config_data/faostat_products_name_code_shortname.csv
    depending on the purpose: production or trade

    :parameter table_list (list), list of the tables to retrieve product codes
    :return product_list (list), list of the main Faostat product codes

    """
    # Name of product file to retrieve
    main_product_file = (
        faostat.config_data_dir / "faostat_products_name_code_shortname.csv"
    )
    # Retrieve dataset of regulation products
    main_products = pd.read_csv(main_product_file)
    # Select only product codes with an associated commodity from the commodity datataset
    main_products = (
        main_products[~main_products.commodity_name.isnull()]
        .code.drop_duplicates()
        .to_list()
    )
    # Define db and pre allocate dataframe
    db = faostat.db
    df = pd.DataFrame(columns=["product_code"])
    # Define which products are inside the production/trade list
    for table in table_list:
        table = db.tables[table]
        df_table = pd.read_sql_query(
            table.select()
            .distinct(table.c.product_code)
            .with_only_columns([table.c.product_code]),
            db.engine,
        )
        df = pd.concat([df, df_table], ignore_index=True)
    # Drop db product duplicates
    product_list = df.product_code.drop_duplicates().to_list()
    # Obtain the intersection
    product_list = list(set(main_products).intersection(product_list))
    return product_list


def reporter_iso_codes(df):
    """
    Script which transforms faostat reporter and partner codes into iso3 codes

    :param df (DataFrame), which contains reporter_code and (if applicable) partner_code columns
    :return df (DataFrame), with the substitution into iso3 codes

    """
    # Reporter codes
    reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
    reporter = pd.read_csv(reporter_file)
    # Obtain iso3 codes for reporters and partners
    df = df.merge(
        reporter[["faost_code", "iso3_code"]],
        how="left",
        left_on="reporter_code",
        right_on="faost_code",
    )
    df["reporter_code"] = df["iso3_code"]
    if "partner_code" in df.columns:
        df.drop(columns=["faost_code", "iso3_code"], inplace=True)
        df = df.merge(
            reporter[["faost_code", "iso3_code"]],
            how="left",
            left_on=["partner_code"],
            right_on=["faost_code"],
        )
        df["partner_code"] = df["iso3_code"]
    df.drop(columns=["faost_code", "iso3_code"], inplace=True)
    # Consider only data of official country codes by GISCO
    country_codes = (
        pd.read_csv(
            data_dir / "GISCO_CNTR_LIST.txt",
            sep=";",
        )
        .ISO3_CODE.drop_duplicates()
        .to_list()
    )
    df = df[df.reporter_code.isin(country_codes)].reset_index(drop=True)
    if "partner_code" in df.columns:
        df = df[df.partner_code.isin(country_codes)].reset_index(drop=True)
        # Remove free zones internal trade data
        df = df[df["reporter_code"] != df["partner_code"]].reset_index(
            drop=True
        )
    return df


def merge_faostat_comtrade_data(
    faostat_code=None,
    comtrade_regulation=None,
    aggregate=True,
    nr_products_chunk=10,
):
    """
    Script which merges faostat yearly trade data with comtrade yearly and monthly trade data.
    The last 12 monthly comtrade data are aggregated to reconstruct the most recent year.
    Considered only import and export data. Re-import and re-export excluded.

    :param faostat_code (list), list of product codes to be retrieved
    :param dataframe comtrade_regulation: comtrade regulation codes to be loaded and aggregated, default is None and in this case they are mapped into faostat codes
    :param boolean aggregate: data are aggregated or not by product code, default is True
    :param int nr_products_chunk: split regulation codes into chucks to avoid run out of memory
    :return df_merge (DataFrame), dataframe with merged data

    """
    # Pre allocate dataframe
    df_merge = pd.DataFrame()
    # Select quantities from Faostat db and Comtrade for trade data for all countries (code < 1000)
    if comtrade_regulation is not None:
        regulation_products = comtrade_regulation.product_code.unique().tolist()
    else:
        regulation_products = [None]
    index_list = [
        "source",
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "product",
        "element_code",
        "element",
        "year",
        "unit",
    ]
    # Split product list in chunks to avoid run out of memory
    for i in range(0, len(regulation_products), nr_products_chunk):
        regulation_code = regulation_products[i : i + nr_products_chunk]
        if comtrade_regulation is not None:
            comtrade_code = comtrade_regulation[
                comtrade_regulation.product_code.isin(regulation_code)
            ].comtrade_code.to_list()
        else:
            comtrade_code = None
        df = merge_faostat_comtrade(
            "crop_trade",
            "yearly",
            faostat_code,
            comtrade_code,
            aggregate,
        )
        # Select only import and export (exclude re-import and re-export for now) and remove nan reporters/partners (=-1)
        df = df[
            (
                df.element.isin(
                    [
                        "import_value",
                        "import_quantity",
                        "export_value",
                        "export_quantity",
                    ]
                )
            )
            & (~(df[["reporter_code", "partner_code"]] == -1).any(axis=1))
        ].reset_index(drop=True)
        if comtrade_regulation is not None:
            # Merge to obtain regulation product codes
            df = df.merge(
                comtrade_regulation,
                left_on="product_code",
                right_on="comtrade_code",
                how="left",
                suffixes=("", "_regulation"),
            )
            # Replace comtrade products with product code regulations to aggregate
            selector = df.source == "comtrade"
            df.loc[selector, "product"] = df.loc[selector, "product_name"]
            df.loc[selector, "product_code"] = df.loc[
                selector, "product_code_regulation"
            ]
            df = df.groupby(index_list)["value"].agg(sum).reset_index()
        df_merge = pd.concat([df_merge, df], ignore_index=True)
        # Avoid to retrieve for all the cycle the same faostat data
        if i == 0:
            faostat_code = None
    # Use period instead of year column
    df_merge["period"] = df_merge["year"]
    return df_merge


def average_results(df, threshold, dict_list, interval_array=np.array([])):
    """
    Script which produce the average and percentage results for the tree maps of the web platform

    :param df (DataFrame), database to perform calculations
    :param threshold (int), percentage above which classifying as "Others" the percentages in the tree maps
    :param dict_list (list), list of dictionaries containing the column names for the aggregation and the percentages, as well the code for the "Others" category and possible columns to be added to the group by aggregation
    :param interval_array (array), array with incremental weights (between 0 and 1) to assign data intervals. If empty (default), intervals are not calculated
    :return df_final (DataFrame), where calculations are performed

    """
    # Make a copy of df to avoid overrides
    df = df.copy()
    # Query df to obtain most recent year of data
    most_recent_year = sorted(df.year.unique(), reverse=True)[0]
    # Consider all the years of the table except the most recent (no aggregation for it)
    years = sorted(df.year.unique(), reverse=True)[1:]
    # Define aggregation of 5 years at a time, starting from the most recent year - 1
    periods = np.array(range(0, len(years))) // 5 + 1
    # Construct the df_periods containing the periods yyyy-yyyy and merge it with df
    df_periods = pd.DataFrame(
        {
            "year": [most_recent_year, *years],
            "period_aggregation": [0, *periods],
        }
    )
    # Define the min year inside each period
    df_periods_min = (
        df_periods.groupby(["period_aggregation"])
        .agg({"year": "min"})
        .reset_index()
    )
    # Define the max year inside each period
    df_periods_max = (
        df_periods.groupby(["period_aggregation"])
        .agg({"year": "max"})
        .reset_index()
    )
    # Merge on the periods
    df_periods = df_periods.merge(
        df_periods_min,
        how="left",
        on="period_aggregation",
        suffixes=("", "_min"),
    )
    df_periods = df_periods.merge(
        df_periods_max,
        how="left",
        on="period_aggregation",
        suffixes=("", "_max"),
    )
    # Define the structure yyyy-yyyy for the period aggregation column
    df_periods["period_aggregation"] = (
        df_periods["year_min"].astype(str)
        + "-"
        + df_periods["year_max"].astype(str)
    )
    # Assign the associated period to each data
    df = df.merge(
        df_periods[["year", "period_aggregation"]], on="year", how="left"
    )
    df["period"] = df["period_aggregation"]
    # Default index list for aggregations + the adds from the arguments
    index_list = ["element", "period", "unit"]
    df_final = pd.DataFrame()
    column_drop = []
    # Load for each dict the aggregation column, the percentage column and the code for threshold values in order to compute calculations
    for dict in dict_list:
        index_list_upd = [
            dict["average_col"],
            *index_list,
            *dict["index_list_add"],
        ]
        value_col_name = "value"
        average_col_name = dict["average_col"] + COLUMN_AVG_SUFFIX
        total_col_name = dict["average_col"] + COLUMN_TOT_SUFFIX
        percentage_col_name = dict["percentage_col"] + COLUMN_PERC_SUFFIX
        other_code = dict["threshold_code"]
        # For the aggregation column calculate mean and sum of the values in the given period aggregation, related to a specific unit and element
        df_mean = (
            df.groupby([*index_list_upd, "year"])
            .agg({"value": "sum"})
            .reset_index()
            .groupby(index_list_upd)
            .agg({"value": "mean"})
            .reset_index()
            .rename(columns={"value": average_col_name})
        )
        df_total = (
            df.groupby(index_list_upd)
            .agg({"value": "sum"})
            .reset_index()
            .rename(columns={"value": total_col_name})
        )
        if total_col_name not in column_drop:
            column_drop.append(total_col_name)
        # Percentage column aggregated with the column list
        df_new = (
            df.groupby([*index_list_upd, dict["percentage_col"]])
            .agg({"value": "sum"})
            .reset_index()
        )
        if value_col_name not in column_drop:
            column_drop.append(value_col_name)
        # Merge with mean and total values
        df_new = df_new.merge(df_mean, how="left", on=index_list_upd)
        df_new = df_new.merge(df_total, how="left", on=index_list_upd)
        # Percentage associated to the percentage column on a given aggregated period
        df_new[percentage_col_name] = (
            df_new["value"] / df_new[total_col_name] * 100
        )
        # Sort by percentage, compute the cumulative sum and shift it by one
        df_new.sort_values(
            by=[*index_list_upd, percentage_col_name],
            ascending=False,
            inplace=True,
            ignore_index=True,
        )
        # Skip nan values is True for cumsum by default
        df_new["cumsum"] = df_new.groupby(index_list_upd)[
            percentage_col_name
        ].cumsum()
        df_new["cumsum_lag"] = df_new.groupby(index_list_upd)[
            "cumsum"
        ].transform("shift", fill_value=0)
        # Create a grouping variable instead of the percentage column, which will be 'Others' for
        # values above the threshold
        df_new[dict["percentage_col"]] = df_new[dict["percentage_col"]].where(
            df_new["cumsum_lag"] < threshold, other_code
        )
        # Group the percentage column values which are in the 'Others' category and calculate their percentage
        df_new = (
            df_new.groupby([*index_list_upd, dict["percentage_col"]])
            .agg(
                {
                    "value": "sum",
                    average_col_name: "first",
                    total_col_name: "first",
                }
            )
            .reset_index()
        )
        df_new[percentage_col_name] = (
            df_new["value"] / df_new[total_col_name] * 100
        )
        if df_final.empty:
            df_final = df_new
        else:
            # Merge or concatenate depending on the common columns in merge_col_list
            merge_col_list = [*index_list_upd, dict["percentage_col"]]
            if average_col_name in df_final.columns:
                merge_col_list.append(average_col_name)
            # Meaning that dataframes can be merged
            if set(merge_col_list).issubset(df_final.columns):
                merge_suffix = "_merge"
                df_final = df_final.merge(
                    df_new,
                    how="outer",
                    on=merge_col_list,
                    suffixes=("", merge_suffix),
                )
                # Add merge columns to column drop list
                for col in df_final.columns:
                    if col.endswith(merge_suffix):
                        if col not in column_drop:
                            column_drop.append(col)
            else:
                df_final = pd.concat([df_final, df_new], ignore_index=True)
    # Remove columns from df_final
    df_final.drop(columns=column_drop, inplace=True)
    # Compute avg production for a certain commodity, reporter, (partner) and period
    groupby_avg_cols = [
        "product_code",
        "element",
        "unit",
        "reporter_code",
        "period",
    ]
    if "partner_code" in df.columns:
        groupby_avg_cols.append("partner_code")
    # Calculate the average over time
    df_avg = (
        df.groupby(groupby_avg_cols)
        .agg({"value": "mean"})
        .reset_index()
        .rename(columns={"value": "avg_value"})
    )
    if len(interval_array):
        # Extract max value avg production for a commodity across periods and countries
        groupby_max_cols = ["product_code", "element", "unit"]
        df_max = (
            df_avg.groupby(groupby_max_cols)
            .agg({"avg_value": "max"})
            .reset_index()
            .rename(columns={"avg_value": "max_avg_value"})
        )
        df_avg = df_avg.merge(df_max, how="left", on=groupby_max_cols)
        # Compute thresholds: [interval_array] * df["max_avg_value"]
        df_avg["bin"] = (
            df_avg["max_avg_value"].values.reshape(len(df_avg), 1)
            * np.array([interval_array])
        ).tolist()
        # For each group (product, element, unit) define to which interval the average production of the specific country and period belongs
        df_groups = df_avg.groupby(groupby_max_cols)
        df_avg = pd.DataFrame()
        df_legend = pd.DataFrame()
        for key in df_groups.groups.keys():
            df_key = df_groups.get_group(key).reset_index(drop=True)
            key_legend = pd.DataFrame()
            bins = df_key.bin[0]
            # If no max_avg_production defined, then put nan
            if df_key.max_avg_value.isnull().all():
                df_key["interval"] = pd.cut(
                    df_key["avg_value"],
                    bins=bins,
                    duplicates="drop",
                )
            # Assign the intervals and ranges
            elif len(df_key.max_avg_value.unique()) == 1:
                df_key["interval"] = pd.cut(
                    df_key["avg_value"],
                    bins=bins,
                    labels=list(range(0, len(interval_array) - 1)),
                )
                df_key["interval_range"] = pd.cut(
                    df_key["avg_value"],
                    bins=bins,
                )
                key_legend["interval"] = df_key[
                    "interval"
                ].cat.categories.values
                key_legend["min_value"] = df_key[
                    "interval_range"
                ].cat.categories.left
                key_legend["max_value"] = df_key[
                    "interval_range"
                ].cat.categories.right
                key_legend["product_code"] = key[0]
                key_legend["element"] = key[1]
                key_legend["unit"] = key[2]
            # Inconsistencies could be detected with the warning
            else:
                faostat.db.logger.warning(
                    f"The dataset represented by {groupby_max_cols} = {key} tuple has not been included into the final csv file due to inconsistencies, please check the data."
                )
                continue
            # Concat to obtain updated dataframes
            df_legend = pd.concat(
                [df_legend, key_legend],
                ignore_index=True,
            )
            df_avg = pd.concat(
                [df_avg, df_key[[*groupby_avg_cols, "avg_value", "interval"]]],
                ignore_index=True,
            )
        # Columns to keep in the legend dataframe
        drop_column = "element"
        column_list = df_legend.columns.tolist()
        column_list.remove(drop_column)
        # Save interval legends
        harvested_area_legend = df_legend[
            df_legend["element"] == "area_harvested"
        ][column_list]
        # Put legend in million hectares
        selector = harvested_area_legend.interval == 0
        harvested_area_legend.loc[selector, "description"] = "up to " + (
            harvested_area_legend.loc[selector, "max_value"] / 10**6
        ).round(2).astype(str)
        harvested_area_legend.loc[~selector, "description"] = (
            "from "
            + (harvested_area_legend.loc[~selector, "min_value"] / 10**6)
            .round(2)
            .astype(str)
            + " to "
            + (harvested_area_legend.loc[~selector, "max_value"] / 10**6)
            .round(2)
            .astype(str)
        )
        harvested_area_legend["description"] = (
            harvested_area_legend["description"]
            + " M"
            + harvested_area_legend.unit
        )
        save_file(harvested_area_legend, "harvested_area_average_legend.csv")
        production_legend = df_legend[
            df_legend["element"].isin(["production", "stocks"])
        ][column_list]
        # Put legend in Million or kilo (for products 839 and 869) tonnes, m3 and heads
        selector = (production_legend.interval == 0) & (
            production_legend.product_code.isin([839, 869])
        )
        production_legend.loc[selector, "description"] = (
            "up to "
            + (production_legend.loc[selector, "max_value"] / 10**3)
            .round(2)
            .astype(str)
            + " k"
        )
        selector = (production_legend.interval == 0) & ~(
            production_legend.product_code.isin([839, 869])
        )
        production_legend.loc[selector, "description"] = (
            "up to "
            + (production_legend.loc[selector, "max_value"] / 10**6)
            .round(2)
            .astype(str)
            + " M"
        )
        selector = (production_legend.interval != 0) & (
            production_legend.product_code.isin([839, 869])
        )
        production_legend.loc[selector, "description"] = (
            "from "
            + (production_legend.loc[selector, "min_value"] / 10**3)
            .round(2)
            .astype(str)
            + " to "
            + (production_legend.loc[selector, "max_value"] / 10**3)
            .round(2)
            .astype(str)
            + " k"
        )
        selector = (production_legend.interval != 0) & ~(
            production_legend.product_code.isin([839, 869])
        )
        production_legend.loc[selector, "description"] = (
            "from "
            + (production_legend.loc[selector, "min_value"] / 10**6)
            .round(2)
            .astype(str)
            + " to "
            + (production_legend.loc[selector, "max_value"] / 10**6)
            .round(2)
            .astype(str)
            + " M"
        )
        production_legend["description"] = (
            production_legend["description"] + production_legend["unit"]
        )
        save_file(production_legend, "production_average_legend.csv")
    # Associate the avg productions (eventually with intervals) to the final dataframe
    df_final = df_final.merge(df_avg, on=groupby_avg_cols, how="left")
    return df_final


def aggregated_data(df, code_list, agg_country_code, agg_country_name):
    """
    Script that aggregates country data in order to overcome inconsistencies

    :param df (DataFrame), which contains production or trade data
    :param code_list (list), country codes to be aggregated
    :param agg_country_code (int), country code to be assigned for the aggregation
    :param agg_country_name (string), country name to be assigned for the aggregation
    :return df (DataFrame), with aggregated data

    """
    # Trade data
    if "partner_code" in df.columns:
        # Define aggregated data both for reporter and parnter
        df_agg = df[
            df[["reporter_code", "partner_code"]].isin(code_list).any(axis=1)
        ]
        # Avoid counting internal trades
        mask = (df_agg.reporter_code.isin(code_list)) & (
            df_agg.partner_code.isin(code_list)
        )
        df_agg = df_agg[~mask]
        # Remove country code list data from df dataset
        df = df[
            ~(df[["reporter_code", "partner_code"]].isin(code_list)).any(axis=1)
        ]
        # Aggregation on the reporter side
        df_agg_1 = (
            df_agg[df_agg["reporter_code"].isin(code_list)]
            .groupby(
                [
                    "source",
                    "partner_code",
                    "partner",
                    "product_code",
                    "product",
                    "element_code",
                    "element",
                    "year",
                    "unit",
                ]
            )
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)})
            .reset_index()
        )
        df_agg_1["reporter_code"] = agg_country_code
        df_agg_1["reporter"] = agg_country_name
        # Concat with reporter codes not in country code list
        df_agg_1 = pd.concat(
            [df_agg[~df_agg["reporter_code"].isin(code_list)], df_agg_1],
            ignore_index=True,
        )
        # Aggregation also on the partner side
        df_agg_2 = (
            df_agg_1[df_agg_1["partner_code"].isin(code_list)]
            .groupby(
                [
                    "source",
                    "reporter_code",
                    "reporter",
                    "product_code",
                    "product",
                    "element_code",
                    "element",
                    "year",
                    "unit",
                ]
            )
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)})
            .reset_index()
        )
        df_agg_2["partner_code"] = agg_country_code
        df_agg_2["partner"] = agg_country_name
        # Concat with partner codes not in country code list
        df_agg_2 = pd.concat(
            [
                df_agg_1[~df_agg_1["partner_code"].isin(code_list)],
                df_agg_2,
            ],
            ignore_index=True,
        )
        df_agg = df_agg_2
    # Production data
    else:
        # Produce reporter aggregated data
        df_agg = df[df["reporter_code"].isin(code_list)]
        # Remove country code list data from df dataset
        df = df[~(df["reporter_code"].isin(code_list))]
        df_agg = (
            df_agg.groupby(
                [
                    "product_code",
                    "product",
                    "element_code",
                    "element",
                    "year",
                    "unit",
                ]
            )
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)}).reset_index()
        )
        df_agg["reporter_code"] = agg_country_code
        df_agg["reporter"] = agg_country_name
    # Fill period column
    df_agg["period"] = df_agg["year"]
    # Build the final dataset to return
    df = pd.concat(
        [df, df_agg],
        ignore_index=True,
    )
    return df


def trend_analysis(
    df_data, multi_process=False, groupby_column_list=[], value_column=None
):
    """
    Script which performs the trend analysis

    :param df_data (DataFrame), data on which perform the analysis
    :param multi_process (Boolean), if True segmented regression is performed through multiple cores. Default is False
    :param groupby_column_list (List), columns to be grouped. Default is empty list
    :param value_column (string), column which refers to data. Default is None
    :return df (DataFrame), dataframe containing the relative, absolute and segmented regresssion indicators

    """
    # Calculate the absolute and relative change
    df_data_change = relative_absolute_change(
        df_data,
        last_value=True,
        groupby_column_list=groupby_column_list,
        value_column=value_column,
        multi_process=multi_process,
    )
    # Use as objective function the coefficient of determination (R2), significance level of 0.05 and at least 7 points for the linear regression
    df_data_regression = segmented_regression(
        df_data,
        last_value=True,
        function="R2",
        alpha=0.05,
        min_data_points=7,
        multi_process=multi_process,
        groupby_column_list=groupby_column_list,
        value_column=value_column,
    )
    # Merge dataframes to compare results
    df = merge_analysis(
        df_change=df_data_change, df_segmented_regression=df_data_regression
    )
    # Define the structure yyyy-yyyy for the period change and segmented regression columns
    df["period_change"] = (
        df["year_range_lower_change"].astype("Int64").astype(str)
        + "-"
        + df["year_range_upper_change"].astype("Int64").astype(str)
    ).replace("<NA>-<NA>", np.nan)
    df["period_regression"] = (
        df["year_range_lower_regression"].astype("Int64").astype(str)
        + "-"
        + df["year_range_upper_regression"].astype("Int64").astype(str)
    ).replace("<NA>-<NA>", np.nan)
    # Transform from boolean to integers 0 or 1
    df["mk_significance_flag"] = (
        df["mk_ha_test"].astype("boolean").astype("Int64")
    ).replace(pd.NA, np.nan)
    # Transform unit in unit / year
    df["unit"] = df["unit"] + "/year"
    return df
