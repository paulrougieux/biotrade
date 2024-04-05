"""
Written by Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script used to compute averages and percentages of trade quantities related to the main commodities, europe and rest of the world imports

"""


def main():
    from scripts.front_end.functions import (
        main_product_list,
        comtrade_products,
        merge_faostat_comtrade_data,
        filter_trade_data,
        aggregated_data,
        reporter_iso_codes,
        average_results,
        replace_zero_with_nan_values,
        save_file,
    )
    from scripts.front_end.functions import (
        COLUMN_PERC_SUFFIX,
        COLUMN_AVG_SUFFIX,
    )
    from biotrade.faostat import faostat
    from biotrade.faostat.aggregate import agg_trade_eu_row
    import pandas as pd

    # Obtain faostat product codes
    faostat_list = main_product_list(["crop_trade"])
    # Obtain comtrade regulation codes
    comtrade_regulation = comtrade_products()
    # Trade data related to product code list
    trade_data = merge_faostat_comtrade_data(
        faostat_list, comtrade_regulation, aggregate=False
    )
    # Filter and change units
    trade_data = filter_trade_data(trade_data)
    # Aggregate french territories values to France and add them to the dataframe
    code_list = [68, 69, 87, 135, 182, 270, 281]
    agg_country_code = 68
    agg_country_name = faostat.country_groups.df
    agg_country_name = agg_country_name[
        agg_country_name.faost_code == agg_country_code
    ].fao_table_name.values[0]
    trade_data = aggregated_data(
        trade_data, code_list, agg_country_code, agg_country_name
    )
    # Substitute faostat codes with iso3 codes
    trade_data = reporter_iso_codes(trade_data)
    # Define the columns and codes for the average calculations
    dict_list = [
        {
            "average_col": "product_code",
            "percentage_col": "partner_code",
            "threshold_code": "OTH",
            "index_list_add": ["reporter_code"],
        },
        {
            "average_col": "product_code",
            "percentage_col": "reporter_code",
            "threshold_code": "OTH",
            "index_list_add": ["partner_code"],
        },
    ]
    # Consider faostat data
    df_faostat = trade_data[trade_data["source"] == "faostat"]
    # Calculate the averages and percentages
    df_faostat = average_results(
        df_faostat,
        100,
        dict_list,
    )
    # Consider comtrade data (excluded estimated values)
    df_comtrade = trade_data[trade_data["source"] == "comtrade"]
    # Calculate the averages and percentages
    df_comtrade = average_results(
        df_comtrade,
        100,
        dict_list,
    )
    drop_column_list = [
        "element",
        dict_list[1]["percentage_col"] + COLUMN_PERC_SUFFIX,
    ]
    column_list = df_faostat.columns.tolist()
    for drop_column in drop_column_list:
        column_list.remove(drop_column)
    # Define dropna columns
    dropna_col = [
        col
        for col in column_list
        if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
    ]
    # Remove columns not needed for front end
    column_list.remove("product_code_avg_value")
    # Consider selected columns of import quantities and save the file (drop nan)
    df_faostat_avg_imp = df_faostat.copy()
    df_faostat_avg_imp = replace_zero_with_nan_values(df_faostat_avg_imp, dropna_col)
    df_faostat_avg_imp = df_faostat_avg_imp.dropna(subset=dropna_col)
    df_faostat_reporter = df_faostat_avg_imp[
        df_faostat_avg_imp["element"] == "import_quantity"
    ][column_list]
    save_file(df_faostat_reporter, "faostat_average.csv")
    # Consider selected columns of import quantities and save the file (drop nan)
    df_comtrade_avg_imp = df_comtrade.copy()
    df_comtrade_avg_imp = replace_zero_with_nan_values(df_comtrade_avg_imp, dropna_col)
    df_comtrade_avg_imp = df_comtrade_avg_imp.dropna(subset=dropna_col)
    df_comtrade_reporter = df_comtrade_avg_imp[
        df_comtrade_avg_imp["element"] == "import_quantity"
    ][column_list]
    save_file(df_comtrade_reporter, "comtrade_average.csv")
    # drop_column_list = [
    #     "element",
    #     dict_list[0]["percentage_col"] + COLUMN_PERC_SUFFIX,
    # ]
    # column_list = df_faostat.columns.tolist()
    # for drop_column in drop_column_list:
    #     column_list.remove(drop_column)
    # # Define dropna columns
    # dropna_col = [
    #     col
    #     for col in column_list
    #     if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
    # ]
    # # Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
    # df_faostat_avg_exp = df_faostat.copy()
    # df_faostat_avg_exp = replace_zero_with_nan_values(df_faostat_avg_exp, dropna_col)
    # df_faostat_avg_exp = df_faostat_avg_exp.dropna(subset=dropna_col)
    # df_faostat_partner = df_faostat_avg_exp[
    #     df_faostat_avg_exp["element"] == "export_quantity"
    # ][column_list]
    # save_file(df_faostat_partner, "faostat_average_mf.csv")
    # # Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
    # df_comtrade_avg_exp = df_comtrade.copy()
    # df_comtrade_avg_exp = replace_zero_with_nan_values(df_comtrade_avg_exp, dropna_col)
    # df_comtrade_avg_exp = df_comtrade_avg_exp.dropna(subset=dropna_col)
    # df_comtrade_partner = df_comtrade_avg_exp[
    #     df_comtrade_avg_exp["element"] == "export_quantity"
    # ][column_list]
    # save_file(df_comtrade_partner, "comtrade_average_mf.csv")
    # Consider averages for EU and rest of the world partners
    # Aggregate data with reporters as eu and row
    df_group_reporter = agg_trade_eu_row(
        trade_data,
        grouping_side="reporter",
        drop_index_col=["flag"],
    )
    # Aggregate data with partners as eu and row
    df_group_partner = agg_trade_eu_row(
        trade_data,
        grouping_side="partner",
        drop_index_col=["flag"],
    )
    # Concatenate in a unique df
    df_group = pd.concat(
        [df_group_reporter, df_group_partner],
        ignore_index=True,
    )
    # Substitute with name and codes of the aggregations for the web platform
    selector = df_group["reporter"] == "eu"
    df_group.loc[selector, "reporter_code"] = "EU27"
    selector = df_group["reporter"] == "row"
    df_group.loc[selector, "reporter_code"] = "ROW"
    selector = df_group["partner"] == "eu"
    df_group.loc[selector, "partner_code"] = "EU27"
    selector = df_group["partner"] == "row"
    df_group.loc[selector, "partner_code"] = "ROW"
    df_group[["reporter", "partner"]] = df_group[["reporter", "partner"]].replace(
        ["eu", "row"], ["European Union", "Rest Of the World"]
    )
    # Consider faostat data
    df_faostat = df_group[df_group["source"] == "faostat"]
    # Calculate the averages and percentages
    df_faostat = average_results(
        df_faostat,
        100,
        dict_list,
    )
    # Consider comtrade data (excluded estimated values)
    df_comtrade = df_group[df_group["source"] == "comtrade"]
    # Calculate the averages and percentages
    df_comtrade = average_results(
        df_comtrade,
        100,
        dict_list,
    )
    drop_column_list = [
        "element",
        dict_list[1]["percentage_col"] + COLUMN_PERC_SUFFIX,
    ]
    column_list = df_faostat.columns.tolist()
    for drop_column in drop_column_list:
        column_list.remove(drop_column)
    # Define dropna columns
    dropna_col = [
        col
        for col in column_list
        if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
    ]
    # Remove columns not needed for front end
    column_list.remove("product_code_avg_value")
    # Consider selected columns of import quantities and save the file (drop nan)
    df_faostat_avg_imp = df_faostat.copy()
    df_faostat_avg_imp = replace_zero_with_nan_values(df_faostat_avg_imp, dropna_col)
    df_faostat_avg_imp = df_faostat_avg_imp.dropna(subset=dropna_col)
    df_faostat_reporter = df_faostat_avg_imp[
        (df_faostat_avg_imp["element"] == "import_quantity")
        & (df_faostat_avg_imp["reporter_code"].isin(["EU27", "ROW"]))
    ][column_list]
    save_file(df_faostat_reporter, "faostat_average_eu_row.csv")
    # Consider selected columns of import quantities and save the file (drop nan)
    df_comtrade_avg_imp = df_comtrade.copy()
    df_comtrade_avg_imp = replace_zero_with_nan_values(df_comtrade_avg_imp, dropna_col)
    df_comtrade_avg_imp = df_comtrade_avg_imp.dropna(subset=dropna_col)
    df_comtrade_reporter = df_comtrade_avg_imp[
        (df_comtrade_avg_imp["element"] == "import_quantity")
        & (df_comtrade_avg_imp["reporter_code"].isin(["EU27", "ROW"]))
    ][column_list]
    save_file(df_comtrade_reporter, "comtrade_average_eu_row.csv")
    # drop_column_list = [
    #     "element",
    #     dict_list[0]["percentage_col"] + COLUMN_PERC_SUFFIX,
    # ]
    # column_list = df_faostat.columns.tolist()
    # for drop_column in drop_column_list:
    #     column_list.remove(drop_column)
    # # Define dropna columns
    # dropna_col = [
    #     col
    #     for col in column_list
    #     if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
    # ]
    # # Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
    # df_faostat_avg_exp = df_faostat.copy()
    # df_faostat_avg_exp = replace_zero_with_nan_values(df_faostat_avg_exp, dropna_col)
    # df_faostat_avg_exp = df_faostat_avg_exp.dropna(subset=dropna_col)
    # df_faostat_partner = df_faostat_avg_exp[
    #     (df_faostat_avg_exp["element"] == "export_quantity")
    #     & (df_faostat_avg_exp["partner_code"].isin(["EU27", "ROW"]))
    # ][column_list]
    # save_file(df_faostat_partner, "faostat_average_eu_row_mf.csv")
    # # Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
    # df_comtrade_avg_exp = df_comtrade.copy()
    # df_comtrade_avg_exp = replace_zero_with_nan_values(df_comtrade_avg_exp, dropna_col)
    # df_comtrade_avg_exp = df_comtrade_avg_exp.dropna(subset=dropna_col)
    # df_comtrade_partner = df_comtrade_avg_exp[
    #     (df_comtrade_avg_exp["element"] == "export_quantity")
    #     & (df_comtrade_avg_exp["partner_code"].isin(["EU27", "ROW"]))
    # ][column_list]
    # save_file(df_comtrade_partner, "comtrade_average_eu_row_mf.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
