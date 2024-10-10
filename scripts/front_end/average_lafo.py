"""
Written by Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script used to compute averages of lafo quantities related to the main commodities for single countries, Europe and Rest of the World

"""


def main():
    from scripts.front_end.functions import (
        aggregated_data,
        reporter_iso_codes,
        average_lafo,
        replace_zero_with_nan_values,
        save_file,
    )

    # from biotrade.faostat.aggregate import agg_trade_eu_row
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


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
