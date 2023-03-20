"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data related to all commodities and tree of the key products, for the web platform

"""


def main():
    import pandas as pd
    from biotrade.faostat import faostat
    from scripts.front_end.functions import (
        save_file,
        main_product_list,
        comtrade_products,
    )

    # Name of product file to retrieve
    faostat_commodity_file = (
        faostat.config_data_dir / "faostat_products_name_code_shortname.csv"
    )
    # Retrieve commodity dataset
    df = pd.read_csv(faostat_commodity_file)
    # Obtain the main product codes of production
    main_products = main_product_list(
        ["crop_production", "forestry_production"]
    )
    # Select only production products
    production_products = df[df.code.isin(main_products)]
    # Filter and rename columns
    column_rename_dict = {
        "code": "product_code",
        "product_short_name_viz": "product_name",
        "commodity_name": "commodity_name",
    }
    production_products = production_products[column_rename_dict.keys()]
    production_products = production_products.rename(columns=column_rename_dict)
    # Save csv files to env variable path or into biotrade data folder
    save_file(production_products, "product_list.csv")
    # Obtain the main product codes of trade
    main_products = main_product_list(["crop_trade"])
    # Select only trade products
    faostat_products = df[df.code.isin(main_products)]
    faostat_products = faostat_products[column_rename_dict.keys()]
    faostat_products = faostat_products.rename(columns=column_rename_dict)
    # Save csv files to env variable path or into biotrade data folder
    save_file(faostat_products, "faostat_product_list.csv")
    # Retrieve regulation products and save the file
    comtrade_products = comtrade_products()
    columns = ["product_code", "product_name", "commodity_name"]
    comtrade_products = comtrade_products.drop_duplicates(subset=columns)[
        columns
    ].reset_index(drop=True)
    save_file(comtrade_products, "comtrade_product_list.csv")


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
