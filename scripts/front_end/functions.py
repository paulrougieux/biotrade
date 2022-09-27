"""
Written by Selene Patani.

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


def main_product_list():
    """
    Return the main list of products (without duplicates) contained into the file biotade/config_data/faostat_commodity_tree.csv

    :return product_list (list), list of the main product codes

    """
    # Name of product file to retrieve
    main_product_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
    # Retrieve dataset
    df = pd.read_csv(main_product_file)
    # Retrieve parent and child codes
    parent_codes = df["parent_code"].unique().tolist()
    child_codes = df["child_code"].unique().tolist()
    # Union of the codes without repetitions
    product_list = np.unique(parent_codes + child_codes).tolist()
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
        reporter[["faost_code", "iso3_code", "fao_status_info"]],
        how="left",
        left_on="reporter_code",
        right_on="faost_code",
    )
    df["reporter_code"] = df["iso3_code"]
    # Remove data of old reporters
    df = df[df.fao_status_info != "old"]
    subset_col = ["reporter_code"]
    if "partner_code" in df.columns:
        df.drop(columns=["faost_code", "iso3_code", "fao_status_info"], inplace=True)
        df = df.merge(
            reporter[["faost_code", "iso3_code", "fao_status_info"]],
            how="left",
            left_on=["partner_code"],
            right_on=["faost_code"],
        )
        df["partner_code"] = df["iso3_code"]
        # Remove data of old partners
        df = df[df.fao_status_info != "old"]
        subset_col.append("partner_code")
    # Faostat code 41 (China mainland) and 351 (China mainland + Hong Kong + Macao + Taiwan ) are not mapped into ISO 3 Codes
    df.dropna(subset=subset_col, inplace=True)
    return df


def merge_faostat_comtrade_data(product_list):
    """
    Script which merges faostat yearly trade data with comtrade yearly and monthly trade data.
    The last 12 monthly comtrade data are aggregated to reconstruct the most recent year.
    Considered only import and export data. Re-import and re-export excluded.

    :param product_list (list), list of product codes to be retrieved
    :return df (DataFrame), dataframe with merged data

    """
    # Select quantities from Faostat db and Comtrade for trade data for all countries (code < 1000)
    df_yearly = merge_faostat_comtrade("crop_trade", "yearly", product_list)
    df_monthly = merge_faostat_comtrade("crop_trade", "monthly", product_list)
    # Merge both yearly and aggregated monthly data
    merge_index_list = [
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
    # Merge with indicator argument which produces a new category column _merge: "left_only", "both", "right_only"
    df = df_yearly.merge(
        df_monthly,
        how="outer",
        on=merge_index_list,
        suffixes=("_yearly", "_monthly"),
        indicator=True,
    )
    # Remove nan reporters/partners (=-1)
    df = df[~(df[["reporter_code", "partner_code"]] == -1).any(axis=1)]
    # Select only import and export (exclude re-import and re-export for now)
    df = df[
        df["element"].isin(
            ["import_value", "import_quantity", "export_value", "export_quantity"]
        )
    ]
    # Consider monthly aggregations where yearly data are missed and yearly data for the rest
    selector = df["_merge"] == "right_only"
    df.loc[selector, "value"] = df.loc[selector, "value_monthly"]
    df.loc[~selector, "value"] = df.loc[~selector, "value_yearly"]
    # Add a flag to distinguish monthly aggregate estimations
    selector = (df["_merge"] == "right_only") & (df["flag_monthly"] == "estimate")
    df.loc[selector, "estimate_flag"] = 1
    df.loc[~selector, "estimate_flag"] = 0
    # Use period instead of year column
    df["period"] = df["year"]
    df.drop(
        columns=[
            "value_monthly",
            "value_yearly",
            "flag_monthly",
            "flag_yearly",
            "_merge",
        ],
        inplace=True,
    )
    return df


def average_results(df, threshold, dict_list, interval_array=np.array([])):
    """
    Script which produce the average and percentage results for the tree maps of the web platform

    :param df (DataFrame), database to perform calculations
    :param threshold (int), percentage above which classifying as "Others" the percentages in the tree maps
    :param dict_list (list), list of dictionaries containing the column names for the aggregation and the percentages, as well the code for the "Others" category and possible columns to be added to the group by aggregation
    :param interval_array (array), array with incremental weights (between 0 and 1) to assign data intervals. If empty (default), intervals are not calculated
    :return df_final (DataFrame), where calculations are performed

    """
    # Query df to obtain most recent year of data
    most_recent_year = sorted(df.year.unique(), reverse=True)[0]
    # Consider all the years of the table except the most recent (no aggregation for it)
    years = sorted(df.year.unique(), reverse=True)[1:]
    # Define aggregation of 5 years at a time, starting from the most recent year - 1
    periods = np.array(range(0, len(years))) // 5 + 1
    # Construct the df_periods containing the periods yyyy-yyyy and merge it with df
    df_periods = pd.DataFrame(
        {"year": [most_recent_year, *years], "period_aggregation": [0, *periods]}
    )
    # Define the min year inside each period
    df_periods_min = (
        df_periods.groupby(["period_aggregation"]).agg({"year": "min"}).reset_index()
    )
    # Define the max year inside each period
    df_periods_max = (
        df_periods.groupby(["period_aggregation"]).agg({"year": "max"}).reset_index()
    )
    # Merge on the periods
    df_periods = df_periods.merge(
        df_periods_min, how="left", on="period_aggregation", suffixes=("", "_min")
    )
    df_periods = df_periods.merge(
        df_periods_max, how="left", on="period_aggregation", suffixes=("", "_max")
    )
    # Define the structure yyyy-yyyy for the period aggregation column
    df_periods["period_aggregation"] = (
        df_periods["year_min"].astype(str) + "-" + df_periods["year_max"].astype(str)
    )
    # Assign the associated period to each data
    df = df.merge(df_periods[["year", "period_aggregation"]], on="year", how="left")
    df["period"] = df["period_aggregation"]
    # Default index list for aggregations + the adds from the arguments
    index_list = ["element", "period", "unit"]
    df_final = pd.DataFrame()
    column_drop = []
    # Load for each dict the aggregation column, the percentage column and the code for threshold values in order to compute calculations
    for dict in dict_list:
        index_list_upd = [dict["average_col"], *index_list, *dict["index_list_add"]]
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
        df_new[percentage_col_name] = df_new["value"] / df_new[total_col_name] * 100
        # Sort by percentage, compute the cumulative sum and shift it by one
        df_new.sort_values(
            by=[*index_list_upd, percentage_col_name],
            ascending=False,
            inplace=True,
            ignore_index=True,
        )
        # Skip nan values is True for cumsum by default
        df_new["cumsum"] = df_new.groupby(index_list_upd)[percentage_col_name].cumsum()
        df_new["cumsum_lag"] = df_new.groupby(index_list_upd)["cumsum"].transform(
            "shift", fill_value=0
        )
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
        df_new[percentage_col_name] = df_new["value"] / df_new[total_col_name] * 100
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
                    df_new, how="outer", on=merge_col_list, suffixes=("", merge_suffix)
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
    if len(interval_array):
        # Compute avg production for a certain commodity, reporter and period
        groupby_avg_cols = [
            "product_code",
            "element",
            "unit",
            "reporter_code",
            "period",
        ]
        df_avg = (
            df.groupby(groupby_avg_cols)
            .agg({"value": "mean"})
            .reset_index()
            .rename(columns={"value": "avg_value"})
        )
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
        for key in df_groups.groups.keys():
            df_key = df_groups.get_group(key).reset_index(drop=True)
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
            # Inconsistencies could be detected with the warning
            else:
                faostat.db.logger.warning(
                    f"The dataset represented by {groupby_max_cols} = {key} tuple has not been included into the final csv file due to inconsistencies, please check the data."
                )
                continue
            df_avg = pd.concat([df_avg, df_key], ignore_index=True)
        # Associate the max avg productions with intervals to the final dataframe
        df_final = df_final.merge(df_avg, on=groupby_avg_cols, how="left")
    return df_final


def consistency_check_china_data(df):
    """
    Script that aggregate China Mainland + Tawain data in order to overcome inconsistencies

    :param df (DataFrame), which contains China data
    :return df_china (DataFrame), with aggregate data for China Mainland + Taiwan

    """
    # Trade data
    if "partner_code" in df.columns:
        # Define China data with isocode CHN and Faostat code 357 from China mainland (41) + Taiwan (214) data both from reporter and partner
        df_china = df[df[["reporter_code", "partner_code"]].isin([41, 214]).any(axis=1)]
        # Aggregation on the reporter side
        df_china_1 = (
            df_china[df_china["reporter_code"].isin([41, 214])]
            .groupby(
                [
                    "source",
                    "partner_code",
                    "partner",
                    "product_code",
                    "element",
                    "year",
                    "unit",
                    "estimate_flag",
                ]
            )
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)})
            .reset_index()
        )
        df_china_1["reporter_code"] = 357
        df_china_1["reporter"] = "China mainland and Taiwan"
        # Concat with reporter codes not 41 either 214
        df_china_1 = pd.concat(
            [df_china[~df_china["reporter_code"].isin([41, 214])], df_china_1],
            ignore_index=True,
        )
        # Aggregation also on the partner side
        df_china_2 = (
            df_china_1[df_china_1["partner_code"].isin([41, 214])]
            .groupby(
                [
                    "source",
                    "reporter_code",
                    "reporter",
                    "product_code",
                    "element",
                    "year",
                    "unit",
                    "estimate_flag",
                ]
            )
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)})
            .reset_index()
        )
        df_china_2["partner_code"] = 357
        df_china_2["partner"] = "China mainland and Taiwan"
        # Concat with partner codes not 41 and 214
        df_china_2 = pd.concat(
            [df_china_1[~df_china_1["partner_code"].isin([41, 214])], df_china_2],
            ignore_index=True,
        )
        df_china = df_china_2
    # Production data
    else:
        # Produce China data with isocode CHN and Faostat code 357 from China mainland (41) + Taiwan (214) data
        df_china = df[df["reporter_code"].isin([41, 214])]
        df_china = (
            df_china.groupby(["product_code", "element", "year", "unit"])
            # If all null values, do not return 0 but Nan
            .agg({"value": lambda x: x.sum(min_count=1)}).reset_index()
        )
        df_china["reporter_code"] = 357
        df_china["reporter"] = "China mainland and Taiwan"
    # Fill period column
    df_china["period"] = df_china["year"]
    return df_china


def trend_analysis(df_data):
    """
    Script which performs the trend analysis

    :param df_data (DataFrame), data on which perform the analysis
    :return df (DataFrame), dataframe containing the relative, absolute and segmented regresssion indicators

    """
    # Calculate the absolute and relative change
    df_data_change = relative_absolute_change(df_data, last_value=True)
    # Use as objective function the coefficient of determination (R2), significance level of 0.05 and at least 7 points for the linear regression
    df_data_regression = segmented_regression(
        df_data,
        last_value=True,
        function="R2",
        alpha=0.05,
        min_data_points=7,
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
    return df
