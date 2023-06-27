"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export trends related to harvested area/production of countries associated to the regulation and commodity tree products, for the web platform

"""


def main():
    import sys
    from biotrade.faostat import faostat
    import pandas as pd
    import numpy as np
    from scripts.front_end.functions import (
        aggregated_data,
        main_product_list,
        reporter_iso_codes,
        replace_zero_with_nan_values,
        trend_analysis,
        save_file,
    )

    # Default is using multi process
    multi_process = True
    # If module imported, avoid spawning in Windows
    if __name__ != "__main__":
        if sys.platform.startswith("win"):
            multi_process = False
    # Obtain the main product codes
    main_product_list = main_product_list(
        ["crop_production", "forestry_production"]
    )
    # Select quantities from Faostat db for crop data for all countries (code < 1000)
    crop_data = faostat.db.select(
        table="crop_production",
        product_code=main_product_list,
        element=["production", "area_harvested", "stocks"],
    )
    # Select wood production data
    wood_data = faostat.db.select(
        table="forestry_production",
        product_code=main_product_list,
        element=["production"],
    )
    # Merge data
    crop_data = pd.concat([crop_data, wood_data], ignore_index=True)
    crop_data = crop_data[crop_data["reporter_code"] < 1000]
    # Aggregate french territories values to France and add them to the dataframe
    code_list = [68, 69, 87, 135, 182, 270, 281]
    agg_country_code = 68
    agg_country_name = faostat.country_groups.df
    agg_country_name = agg_country_name[
        agg_country_name.faost_code == agg_country_code
    ].fao_table_name.values[0]
    crop_data = aggregated_data(
        crop_data, code_list, agg_country_code, agg_country_name
    )
    # Substitute faostat codes with iso3 codes
    crop_data = reporter_iso_codes(crop_data)
    # Consider data after 1985 to calculate trends of last year
    crop_data = crop_data[crop_data["year"] > 1985]
    # Perform trend analysis
    df = trend_analysis(crop_data, multi_process=multi_process)
    # Columns to be retained
    column_list = [
        "reporter_code",
        "product_code",
        "period",
        "period_change",
        "period_regression",
        "relative_change",
        "absolute_change",
        "mk_slope",
        "mk_significance_flag",
        "unit",
    ]
    # Drop nan values
    dropna_col = ["relative_change", "absolute_change", "mk_slope"]
    df = replace_zero_with_nan_values(df, dropna_col)
    # Put nan to period and significance columns when relative change or slope is 0
    # To avoid inconsistencies during the replace zero with nan
    selector = df.mk_slope.isnull()
    df.loc[selector, "period_regression"] = np.nan
    df.loc[selector, "mk_significance_flag"] = np.nan
    selector = df.relative_change.isnull() & df.absolute_change.isnull()
    df.loc[selector, "period_change"] = np.nan
    df = df.dropna(subset=dropna_col, how="all")
    # Harvested area data (only the most recent year of db)
    most_recent_year = sorted(df.year.unique(), reverse=True)[0]
    harvested_area = df[
        (df["element"] == "area_harvested") & (df["year"] == most_recent_year)
    ][column_list]
    # Production data (only the most recent year of db)
    production = df[
        (df["element"].isin(["production", "stocks"]))
        & (df["year"] == most_recent_year)
    ][column_list]
    save_file(harvested_area, "harvested_area_trends.csv")
    save_file(production, "production_trends.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
