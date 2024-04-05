"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data of products and countries (averaging over periods of 5 years except last year of db) related to harvested area/production of the regulation and commodity tree products, for the web platform

"""


def main():
    # Import internal dependencies
    import pandas as pd
    import numpy as np
    from biotrade.faostat import faostat
    from scripts.front_end.functions import COLUMN_PERC_SUFFIX
    from scripts.front_end.functions import (
        filter_production_data,
        aggregated_data,
        main_product_list,
        average_results,
        reporter_iso_codes,
        replace_zero_with_nan_values,
        save_file,
    )

    # Obtain the main product codes
    main_product_list = main_product_list(["crop_production", "forestry_production"])
    # Query db to obtain data
    crop_df = faostat.db.select(
        table="crop_production",
        product_code=main_product_list,
        element=["production", "area_harvested", "stocks", "yield"],
    )
    # Select wood production data
    wood_df = faostat.db.select(
        table="forestry_production",
        product_code=main_product_list,
        element=["production"],
    )
    # Merge data
    df = pd.concat([crop_df, wood_df], ignore_index=True)
    # Filter df with the common most recent year of the several element types
    most_recent_year = df.groupby("element")["year"].max().min()
    df = df[df.year <= most_recent_year]
    # Filter at country level and change units
    df = filter_production_data(df)
    # Aggregate french territories values to France and add them to the dataframe
    code_list = [68, 69, 87, 135, 182, 270, 281]
    agg_country_code = 68
    agg_country_name = faostat.country_groups.df
    agg_country_name = agg_country_name[
        agg_country_name.faost_code == agg_country_code
    ].fao_table_name.values[0]
    df = aggregated_data(df, code_list, agg_country_code, agg_country_name)
    # Substitute faostat codes with iso3 codes
    df = reporter_iso_codes(df)
    # Define the columns and codes for the average calculations
    dict_list = [
        {
            "average_col": "reporter_code",
            "percentage_col": "product_code",
            "threshold_code": -1,
            "index_list_add": [],
        },
        {
            "average_col": "product_code",
            "percentage_col": "reporter_code",
            "threshold_code": "OTH",
            "index_list_add": [],
        },
    ]
    # Calculate the averages and percentages with intervals
    intervals = np.concatenate(
        (
            np.array([0.0]),
            np.array(np.linspace(5, 95, 10) / 100),
            np.array([1.0]),
        )
    )
    df_final = average_results(
        df,
        100,
        dict_list,
        intervals,
    )
    # Columns to keep
    drop_column = [
        "element",
        "reporter_code_avg_value",
        "product_code_percentage",
        "product_code_avg_value",
        "reporter_code_percentage",
    ]
    column_list = df_final.columns.tolist()
    for col in drop_column:
        column_list.remove(col)
    # Define dropna columns
    dropna_col = [col for col in df_final.columns if col.endswith(COLUMN_PERC_SUFFIX)]
    df_final = replace_zero_with_nan_values(df_final, dropna_col)
    df_final = df_final.dropna(subset=dropna_col, how="all")
    # Save csv files to env variable path or into biotrade data folder
    harvested_area = df_final[df_final["element"] == "area_harvested"][column_list]
    save_file(harvested_area, "harvested_area_average.csv")
    production = df_final[df_final["element"].isin(["production", "stocks"])][
        column_list
    ]
    save_file(production, "production_average.csv")
    yields = df_final[df_final["element"] == "yield"][column_list]
    save_file(yields, "yield_average.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
