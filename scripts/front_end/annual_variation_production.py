"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data related to harvested area/production of countries associated to the regulation and commodity tree products, for the web platform

"""


def main():
    from scripts.front_end.functions import (
        main_product_list,
        filter_production_data,
        aggregated_data,
        reporter_iso_codes,
        replace_zero_with_nan_values,
        save_file,
    )
    from biotrade.faostat import faostat
    import pandas as pd

    # Obtain the main product codes
    main_product_list = main_product_list(["crop_production", "forestry_production"])
    # Select quantities from Faostat db for crop data for all countries (code < 1000)
    crop_data = faostat.db.select(
        table="crop_production",
        product_code=main_product_list,
        element=["production", "area_harvested", "stocks", "yield"],
    )
    # Select wood production data
    wood_data = faostat.db.select(
        table="forestry_production",
        product_code=main_product_list,
        element=["production"],
    )
    # Merge data
    crop_data = pd.concat([crop_data, wood_data], ignore_index=True)
    # Filter at country level and change units
    crop_data = filter_production_data(crop_data)
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
    # Columns to be retained
    column_list = ["reporter_code", "product_code", "period", "value", "unit"]
    # Harvested area data
    dropna_col = ["value"]
    crop_data = replace_zero_with_nan_values(crop_data, dropna_col)
    crop_data = crop_data.dropna(subset=dropna_col)
    harvested_area = crop_data[crop_data["element"] == "area_harvested"][column_list]
    # Production data
    production = crop_data[crop_data["element"].isin(["production", "stocks"])][
        column_list
    ]
    # Yield data
    yields = crop_data[crop_data["element"] == "yield"][column_list]
    # Save csv files to env variable path or into biotrade data folder
    save_file(harvested_area, "harvested_area_annual_variation.csv")
    save_file(production, "production_annual_variation.csv")
    save_file(yields, "yield_annual_variation.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
