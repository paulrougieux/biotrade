"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script made to export data related to all commodities and tree of the key products, for the web platform land footprint results

"""


def main():
    import pandas as pd
    from scripts.front_end.functions import (
        save_file,
        comtrade_products,
    )

    # Retrieve regulation products
    comtrade_products = comtrade_products()
    # Filter commodity products (exclude rubber derived products)
    removed_commodity_products = ["Rubber"]
    rubber_primary_code = "4001"
    products = pd.concat(
        [
            comtrade_products[
                ~comtrade_products["commodity_name"].isin(removed_commodity_products)
            ],
            comtrade_products[comtrade_products["product_code"] == rubber_primary_code],
        ],
        ignore_index=True,
    ).reset_index(drop=True)
    # Columns to be considered
    columns = ["product_code", "product_name", "commodity_code", "commodity_name"]
    products = products.drop_duplicates(subset=columns)[columns].reset_index(drop=True)
    # Columns to be finally retained
    column_list = ["commodity_code", "commodity_name"]
    save_file(
        products[column_list].drop_duplicates().sort_values(by=["commodity_name"]),
        "lafo_commodity_list.csv",
    )
    # Add "Others" aggregations
    oth_products = products[column_list].drop_duplicates(subset="commodity_code")
    # TODO Replace "999" with "OTH_"
    oth_products["product_code"] = "999" + oth_products["commodity_code"]
    oth_products["product_name"] = "Other products"
    products = pd.concat([products, oth_products], ignore_index=True)
    # Columns to be finally retained
    column_list = ["product_code", "product_name", "commodity_code"]
    save_file(
        products[column_list]
        .drop_duplicates()
        .sort_values(by=["commodity_code", "product_code"]),
        "lafo_product_list.csv",
    )


# Needed to avoid running module when imported
if __name__ == "__main__":
    main()
