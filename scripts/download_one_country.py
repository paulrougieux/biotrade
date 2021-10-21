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


from biotrade.comtrade import comtrade

print(comtrade.data_dir)

# Other sawnwood
selected_countries = ["Ukraine", "USA", "Russian Federation"]
comtrade.countries.reporters.query(f"text in {selected_countries}")
swd99 = comtrade.pump.download(cc="440799")

# Load monthly data as an example
swd99_ukr_202106 = comtrade.pump.download(
    freq="M", r="all", p="804", ps="201906", cc="440799"
)

# Loop on all products under code 44
