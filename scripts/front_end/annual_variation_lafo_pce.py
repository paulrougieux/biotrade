"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data for the EUFO web platform, related to lafo values of countries associated to the regulation and commodity tree products

"""


def main():
    from scripts.front_end.functions import (
        aggregated_data,
        reporter_iso_codes,
        replace_zero_with_nan_values,
        save_file,
    )
    from biotrade.faostat import faostat
    from biotrade.comtrade import comtrade
    from biotrade.faostat.aggregate import agg_trade_eu_row
    import pandas as pd

    # TODO retrieve lafo data from functions (deforestation package), for now reading a csv example file
    lafo_data = pd.read_csv(comtrade.data_dir / "lafo_example.csv")
    # Render columns lower case
    lafo_data.columns = lafo_data.columns.str.lower()
    # Remove columns
    lafo_data.drop(
        columns=[
            "iso2_code",
            "primary_commodity_code",
            "primary_commodity",
            "area_harvested",
            "production",
            "yield",
            "yield_avg",
            "flag_out",
        ],
        inplace=True,
    )
    # Rename to product_code column
    lafo_data.rename(columns={"reg_code": "product_code"}, inplace=True)
    # Add period column
    lafo_data["period"] = lafo_data["year"]
    # Aggregate french territories values to France and add them to the dataframe
    code_list = [68, 69, 87, 135, 182, 270, 281]
    agg_country_code = 68
    agg_country_name = faostat.country_groups.df
    agg_country_name = agg_country_name[
        agg_country_name.faost_code == agg_country_code
    ].fao_table_name.values[0]
    lafo_data = aggregated_data(
        lafo_data,
        code_list,
        agg_country_code,
        agg_country_name,
        groupby_cols=[
            "product_code",
            "year",
        ],
        value_cols=["pce", "lafo_obs"],
    )
    # Substitute faostat codes with iso3 codes
    lafo_data = reporter_iso_codes(lafo_data)
    # Add units
    lafo_data["pce_unit"] = "tonnes"
    lafo_data["lafo_obs_unit"] = "ha"
    # Aggregate to EU and ROW for reporters
    eu_row_data = agg_trade_eu_row(
        lafo_data,
        grouping_side="reporter",
        drop_index_col=[
            "year",
        ],
        value_col=["pce", "lafo_obs"],
    )
    # Substitute with name and codes of the aggregations for the web platform
    selector = eu_row_data["reporter"] == "eu"
    eu_row_data.loc[selector, "reporter_code"] = "EU27"
    eu_row_data.loc[~selector, "reporter_code"] = "ROW"
    eu_row_data["reporter"].replace(
        ["eu", "row"], ["European Union", "Rest Of the World"], inplace=True
    )
    # Columns to be finally retained
    column_list = [
        "reporter_code",
        "partner_code",
        "product_code",
        "period",
        "value",
        "unit",
    ]
    # Consider selected columns of import quantities and values and save the files (drop nan)
    dropna_col = ["value"]
    for dataset in [lafo_data, eu_row_data]:
        for value_col in ["pce", "lafo_obs"]:
            for source in ["faostat", "comtrade"]:
                df = dataset.copy()
                df.rename(
                    columns={value_col: "value", (value_col + "_unit"): "unit"},
                    inplace=True,
                )
                df = replace_zero_with_nan_values(df, dropna_col)
                df = df.dropna(subset=dropna_col)
                # Faostat
                file_name = f"{source}_{value_col.split('_', 1)[0]}_annual_variation"
                if dataset.equals(eu_row_data):
                    file_name = file_name + "_eu_row"
                save_file(
                    df[df["source"] == source][column_list],
                    file_name + ".csv",
                )


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
