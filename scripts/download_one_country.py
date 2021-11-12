#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

The purpose of this script is to load all relevant data for a given country. Starting with Ukraine.
And to store the data in a SQLite database. Then to make the SQLite database dump available.

The dump could be generated with `con.iterdump` from the sqlite3 package as ex plained in
[how to get sql dump of a database created in sqlite using sqlalchemy](https://stackoverflow.com/a/58573487/2641825)

"""
import time
from biotrade.comtrade import comtrade

# Other sawnwood
selected_countries = ["Ukraine", "USA", "Russian Federation"]
comtrade.country_groups.reporters.query(f"text in {selected_countries}")

# Load monthly data as an example
swd99_ukr_202106 = comtrade.pump.download_df(
    freq="M", r="all", p="804", ps="201906", cc="440799"
)
# Save to the database
comtrade.db.append(swd99_ukr_202106, "monthly")
# Save again should return an error because of the unique constraint!
# TODO: Check why the second save doesn't return an error
# Try by appending all codes together and leaving the constraint on a single variable
comtrade.db.append(swd99_ukr_202106, "monthly")


# List of products to download to the database
hs = comtrade.pump.get_parameter_list("classificationHS.json")
selected_hs2 = ("44", "47", "48", "49")
wood_products = hs[hs.id.str.startswith(selected_hs2)].copy()
wood_products.reset_index(drop=True, inplace=True)
# for x in wood_products.text: print(x)

# Loop on all wood products wait 2 seconds between each call
for product_code, text in zip(wood_products.id, wood_products.text):
    time.sleep(2)
    print(f"Downloading \n:{text}\n")
    # Load monthly data as an example
    swd99_ukr_202106 = comtrade.pump.download_df(
        freq="M", r="all", p="804", ps="201906", cc=product_code
    )
    # Save to the database
    comtrade.db.append(swd99_ukr_202106, "monthly")
