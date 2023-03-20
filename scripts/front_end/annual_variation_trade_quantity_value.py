"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data related to trade quantities of countries associated to the regulation and commodity tree products, for the web platform

"""


def main():
    import pandas as pd
    from scripts.front_end.functions import (
        main_product_list,
        comtrade_products,
        merge_faostat_comtrade_data,
        consistency_check_china_data,
        reporter_iso_codes,
        replace_zero_with_nan_values,
        save_file,
    )
    from biotrade.faostat.aggregate import agg_trade_eu_row

    # Obtain faostat product codes
    faostat_list = main_product_list(["crop_trade"])
    # Obtain comtrade regulation codes
    comtrade_regulation = comtrade_products()
    # Trade data related to product code list
    trade_data = merge_faostat_comtrade_data(
        faostat_list, comtrade_regulation, aggregate=False
    )
    # China Mainland + Taiwan data
    df_china = consistency_check_china_data(trade_data)
    # Add China data to trade_data (exclude Taiwan data)
    trade_data = pd.concat(
        [
            trade_data[
                ~(
                    (trade_data[["reporter_code", "partner_code"]] == 214).any(
                        axis=1
                    )
                )
            ],
            df_china,
        ],
        ignore_index=True,
    )
    # Substitute faostat codes with iso3 codes
    trade_data = reporter_iso_codes(trade_data)
    # Columns to be retained
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
    trade_data = replace_zero_with_nan_values(trade_data, dropna_col)
    trade_data = trade_data.dropna(subset=dropna_col)
    df = trade_data[
        (trade_data["source"] == "faostat")
        & (trade_data["element"] == "import_quantity")
    ][column_list]
    save_file(df, "faostat_annual_variation.csv")
    df = trade_data[
        (trade_data["source"] == "faostat")
        & (trade_data["element"] == "import_value")
    ][column_list]
    save_file(df, "faostat_value_annual_variation.csv")
    # Consider selected columns of export quantities and values and save the files (drop nan)
    df = trade_data[
        (trade_data["source"] == "faostat")
        & (trade_data["element"] == "export_quantity")
    ][column_list]
    save_file(df, "faostat_annual_variation_mf.csv")
    df = trade_data[
        (trade_data["source"] == "faostat")
        & (trade_data["element"] == "export_value")
    ][column_list]
    save_file(df, "faostat_value_annual_variation_mf.csv")
    # Consider selected columns of import quantities and values and save the files (drop nan)
    df = trade_data[
        (trade_data["source"] == "comtrade")
        & (trade_data["element"] == "import_quantity")
    ][column_list]
    save_file(df, "comtrade_annual_variation.csv")
    df = trade_data[
        (trade_data["source"] == "comtrade")
        & (trade_data["element"] == "import_value")
    ][column_list]
    save_file(df, "comtrade_value_annual_variation.csv")
    # Consider selected columns of export quantities and values and save the files (drop nan)
    df = trade_data[
        (trade_data["source"] == "comtrade")
        & (trade_data["element"] == "export_quantity")
    ][column_list]
    save_file(df, "comtrade_annual_variation_mf.csv")
    df = trade_data[
        (trade_data["source"] == "comtrade")
        & (trade_data["element"] == "export_value")
    ][column_list]
    save_file(df, "comtrade_value_annual_variation_mf.csv")
    # Aggregate to EU and ROW for reporters
    eu_row_data = agg_trade_eu_row(
        trade_data,
        grouping_side="reporter",
        drop_index_col=[
            "year",
            "flag",
            "faost_code",
            "iso3_code",
            "fao_status_info",
        ],
    )
    # Substitute with name and codes of the aggregations for the web platform
    selector = eu_row_data["reporter"] == "eu"
    eu_row_data.loc[selector, "reporter_code"] = "EU27"
    eu_row_data.loc[~selector, "reporter_code"] = "ROW"
    eu_row_data["reporter"].replace(
        ["eu", "row"], ["European Union", "Rest Of the World"], inplace=True
    )
    # Save imports
    df = eu_row_data[
        (eu_row_data["source"] == "faostat")
        & (eu_row_data["element"] == "import_quantity")
    ][column_list]
    save_file(df, "faostat_annual_variation_eu_row.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "faostat")
        & (eu_row_data["element"] == "import_value")
    ][column_list]
    save_file(df, "faostat_value_annual_variation_eu_row.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "comtrade")
        & (eu_row_data["element"] == "import_quantity")
    ][column_list]
    save_file(df, "comtrade_annual_variation_eu_row.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "comtrade")
        & (eu_row_data["element"] == "import_value")
    ][column_list]
    save_file(df, "comtrade_value_annual_variation_eu_row.csv")
    # Aggregate to EU and ROW for partners
    eu_row_data = agg_trade_eu_row(
        trade_data,
        grouping_side="partner",
        drop_index_col=[
            "year",
            "flag",
            "faost_code",
            "iso3_code",
            "fao_status_info",
        ],
    )
    # Substitute with name and codes of the aggregations for the web platform
    selector = eu_row_data["partner"] == "eu"
    eu_row_data.loc[selector, "partner_code"] = "EU27"
    eu_row_data.loc[~selector, "partner_code"] = "ROW"
    eu_row_data["partner"].replace(
        ["eu", "row"], ["European Union", "Rest Of the World"], inplace=True
    )
    # Save exports
    df = eu_row_data[
        (eu_row_data["source"] == "faostat")
        & (eu_row_data["element"] == "export_quantity")
    ][column_list]
    save_file(df, "faostat_annual_variation_eu_row_mf.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "faostat")
        & (eu_row_data["element"] == "export_value")
    ][column_list]
    save_file(df, "faostat_value_annual_variation_eu_row_mf.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "comtrade")
        & (eu_row_data["element"] == "export_quantity")
    ][column_list]
    save_file(df, "comtrade_annual_variation_eu_row_mf.csv")
    df = eu_row_data[
        (eu_row_data["source"] == "comtrade")
        & (eu_row_data["element"] == "export_value")
    ][column_list]
    save_file(df, "comtrade_value_annual_variation_eu_row_mf.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
