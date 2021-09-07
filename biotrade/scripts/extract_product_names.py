#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Download the list of products (commodities) from Comtrade and reformat it for the purposes of this package.

Display the text description of one commodity as an example of the selection mechanism

>>> print(hs.text[hs.id == "440799"].iloc[0])
"""

# Third party modules
import csv
import pandas

# Internal modules
from env_impact_imports.comtrade import comtrade

# Show more rows
pandas.options.display.max_rows = 100

# Load a configuration file that distinguishes food and non food commodity codes at the 2 digit level
pandas.read_csv(comtrade.config_data_dir / "comtrade_hs_2d.csv")

# Download the list of products (commodities) from the Comtrade
# HS (Harmonized System) classification
hs = comtrade.pump.get_parameter_list("classificationHS.json")

# Export the descriptions at the 2 digit level for later use as a configuration file
hs2d = hs[hs.id.str.len() == 2]
hs2d.to_csv("/tmp/comtrade_hs_2d.csv", index=False, quoting=csv.QUOTE_ALL)

# Write to a temporary csv file for later use as a configuration file
hs.to_csv(
    "/tmp/comtrade_hs_commodities.csv",
    index=False,
    # Quote all columns otherwise cells containing ";" get messed up in some
    # spreadsheet software because it can be considered as an additional csv separator
    quoting=csv.QUOTE_ALL,
)
