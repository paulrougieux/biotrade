"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data for the EUFO web platform, related to annual lafo values of countries associated to the regulation and commodity tree products

"""


def main():
    from scripts.front_end.functions import (
        country_names,
        aggregated_data,
        reporter_iso_codes,
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
    # Aggregate by reporters
    group_by_cols = ["reporter_code", "product_code", "primary_code", "period", "unit"]
    lafo_data = lafo_data.groupby(group_by_cols)["value"].agg("sum").reset_index()
    # Remove nan data
    dropna_col = ["value"]
    lafo_data = replace_zero_with_nan_values(lafo_data, dropna_col)
    lafo_data = lafo_data.dropna(subset=dropna_col)
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "product_code",
        "period",
        "value",
        "unit",
    ]
    # Save data
    save_file(lafo_data[column_list], "lafo_reporter_annual_variation.csv")
    # Aggregate at level of primary code
    group_by_cols.remove("product_code")
    eu_row_data = lafo_data.groupby(group_by_cols)["value"].agg("sum").reset_index()
    # Add reporter and partner names to aggregate at EU, ROW level
    eu_row_data = country_names(eu_row_data, "iso3_code")
    eu_row_data["partner"] = None
    # Aggregate to EU and ROW for reporters
    eu_row_data = agg_trade_eu_row(
        eu_row_data,
        grouping_side="reporter",
    )
    # Calculate the percentage share of EU and ROW
    group_by_cols.remove("reporter_code")
    eu_row_data["sum_value"] = eu_row_data.groupby(group_by_cols)["value"].transform(
        "sum"
    )
    eu_row_data["value_share"] = eu_row_data["value"] / eu_row_data["sum_value"] * 100
    # Substitute with name and codes of the aggregations for the web platform
    selector = eu_row_data["reporter"] == "eu"
    eu_row_data.loc[selector, "reporter_code"] = "EU27"
    eu_row_data.loc[~selector, "reporter_code"] = "ROW"
    # Consider selected columns of import quantities and values and save the files (drop nan)
    dropna_col = ["value"]
    eu_row_data = replace_zero_with_nan_values(eu_row_data, dropna_col)
    eu_row_data = eu_row_data.dropna(subset=dropna_col)
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "commodity_code",
        "period",
        "value",
        "value_share",
        "unit",
    ]
    # Save data
    save_file(
        eu_row_data.rename(columns={"primary_code": "commodity_code"})[column_list],
        "lafo_reporter_annual_variation_eu_row.csv",
    )


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
