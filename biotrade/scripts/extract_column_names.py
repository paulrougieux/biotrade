#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.


"""
# Third party modules
import pandas

# Internal modules
from env_impact_imports.comtrade import comtrade

# Download with human readable headings
swd99_h = comtrade.pump.download(cc="440799", head="H")
# Download with machine readable headings
swd99_m = comtrade.pump.download(cc="440799", head="M")

# Create a CSV file with the 2 headings side by side as columns
machine_names = swd99_m.columns.to_list()
human_names = swd99_h.columns.to_list()
comtrade_cols = pandas.DataFrame(
    {"comtrade_h": human_names, "comtrade_m": machine_names}
)

comtrade_cols.to_csv("/tmp/comtrade_cols.csv", index=False)

# Write sample data to csv
swd99_h.to_csv("/tmp/swd99_h.csv", index=False)
