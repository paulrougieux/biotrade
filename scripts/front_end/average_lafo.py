"""
Written by Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script used to compute averages of lafo quantities related to the main commodities for single countries, Europe and Rest of the World

"""


def main():
    from scripts.front_end.functions import (
        remove_intra_eu_values,
        country_names,
        aggregated_data,
        reporter_iso_codes,
        average_lafo,
        replace_zero_with_nan_values,
        save_file,
    )

    from biotrade.faostat.aggregate import agg_trade_eu_row
    from deforestfoot.crop import Crop

    # Retrieve lafo data from deforestation package
    crop = Crop(commodity_list=["Coffee"])
    lafo_data = crop.lafo.df(flow="apparent_consumption")
    # Add period column
    lafo_data["period"] = lafo_data["year"]
    # Aggregate french territories values to France and add them to the dataframe
    code_list = ["FR", "GF", "GP", "MQ", "RE", "YT", "MF"]
    agg_country_code = "FR"
    agg_country_name = None
    lafo_data = aggregated_data(
        lafo_data,
        code_list,
        agg_country_code,
        agg_country_name,
        groupby_cols=["product_code", "primary_code", "year", "unit"],
    )
    # Substitute iso2 codes with iso3 codes
    lafo_data = reporter_iso_codes(lafo_data, col="iso2_code")
    # Define the columns for the average calculations
    dict_list = ["reporter_code", "partner_code", "primary_code", "period", "unit"]
    # Calculate the averages
    lafo_data_avg = average_lafo(lafo_data, dict_list, "avg_value")
    # Calculate total averages per partner
    dict_list.remove("primary_code")
    lafo_data_avg["partner_total_value"] = lafo_data_avg.groupby(dict_list)[
        "avg_value"
    ].transform("sum")
    # Consider selected columns and save the files (drop nan)
    dropna_col = ["avg_value", "partner_total_value"]
    lafo_data_avg = replace_zero_with_nan_values(lafo_data_avg, dropna_col)
    lafo_data_avg = lafo_data_avg.dropna(subset=dropna_col)
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "partner_code",
        "commodity_code",
        "period",
        "avg_value",
        "partner_total_value",
        "unit",
    ]
    # Save data
    save_file(
        lafo_data_avg.rename(columns={"primary_code": "commodity_code"})[column_list],
        "lafo_average.csv",
    )
    # Define the columns for the average calculations
    dict_list = ["reporter_code", "primary_code", "period", "unit"]
    # Calculate the averages
    lafo_data_avg = average_lafo(lafo_data, dict_list, "avg_value")
    # Calculate total averages
    dict_list = ["primary_code", "period", "unit"]
    lafo_data_avg["commodity_avg_value"] = lafo_data_avg.groupby(dict_list)[
        "avg_value"
    ].transform("sum")
    lafo_data_avg["reporter_value_share"] = (
        lafo_data_avg["avg_value"] / lafo_data_avg["commodity_avg_value"]
    ) * 100
    # Calculate total averages
    dict_list = ["reporter_code", "period", "unit"]
    lafo_data_avg["reporter_avg_value"] = lafo_data_avg.groupby(dict_list)[
        "avg_value"
    ].transform("sum")
    lafo_data_avg["commodity_value_share"] = (
        lafo_data_avg["avg_value"] / lafo_data_avg["reporter_avg_value"]
    ) * 100
    # Consider selected columns and save the files (drop nan)
    dropna_col = ["avg_value", "reporter_value_share", "commodity_value_share"]
    lafo_data_avg = replace_zero_with_nan_values(lafo_data_avg, dropna_col)
    lafo_data_avg = lafo_data_avg.dropna(subset=dropna_col)
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "commodity_code",
        "period",
        "avg_value",
        "reporter_value_share",
        "commodity_value_share",
        "unit",
    ]
    # Save data
    save_file(
        lafo_data_avg.rename(columns={"primary_code": "commodity_code"})[column_list],
        "lafo_reporter_average.csv",
    )
    # Add reporter and partner names to aggregate at EU, ROW level
    eu_row_data = country_names(lafo_data, "iso3_code")
    # Remove EU internal trades
    eu_row_data = remove_intra_eu_values(eu_row_data)
    # Aggregate to EU and ROW for reporters
    eu_row_data = agg_trade_eu_row(
        eu_row_data,
        grouping_side="reporter",
    )
    # Substitute with name and codes of the aggregations for the web platform
    selector = eu_row_data["reporter"] == "eu"
    eu_row_data.loc[selector, "reporter_code"] = "EU27"
    eu_row_data.loc[~selector, "reporter_code"] = "ROW"
    # Calculate the averages
    dict_list = ["reporter_code", "primary_code", "period", "unit"]
    eu_row_data_avg = average_lafo(eu_row_data, dict_list, "avg_value")
    # Calculate the percentage share of EU and ROW
    dict_list = ["primary_code", "period", "unit"]
    eu_row_data_avg["sum_value"] = eu_row_data_avg.groupby(dict_list)[
        "avg_value"
    ].transform("sum")
    eu_row_data_avg["value_share"] = (
        eu_row_data_avg["avg_value"] / eu_row_data_avg["sum_value"] * 100
    )
    # Consider selected columns of import quantities and values and save the files (drop nan)
    dropna_col = ["avg_value", "value_share"]
    eu_row_data_avg = replace_zero_with_nan_values(eu_row_data_avg, dropna_col)
    eu_row_data_avg = eu_row_data_avg.dropna(subset=dropna_col)
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "commodity_code",
        "period",
        "avg_value",
        "value_share",
        "unit",
    ]
    # Save data
    save_file(
        eu_row_data_avg.rename(columns={"primary_code": "commodity_code"})[column_list],
        "lafo_reporter_average_eu_row.csv",
    )


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
