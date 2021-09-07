#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

The purpose of this script is to download yearly Comtrade data at the HS2 level
for all reporter and all partner countries. The data is then stored into a
PostgreSQL database.

"""
# Internal modules
import datetime

# Third party modules

# First party modules
from env_impact_imports.comtrade import comtrade

# Keep only bioeconomy related products
hs2d = comtrade.products.hs2d
hs2d_bio = hs2d[hs2d.bioeconomy == 1]
print(hs2d_bio)

# The main variables along which the download will be spread are:
# - product
# - year
# - reporter country
# - partner country (all)

#########################################
# Prepare query parameters for one step #
#########################################
# Download the last 5 years for one product, one reporter and all its partners
YEAR = datetime.datetime.today().year
# Convert each element of the list to a string
YEARS = [str(YEAR - i) for i in range(1, 6)]
YEARS = ",".join(YEARS)
product_code = "44"
reporter_code = "381"
wood = comtrade.pump.download(
    max="10000",
    type="C",
    freq="A",
    px="HS",
    ps=YEARS,
    r=reporter_code,
    p="all",
    rg="all",
    cc=product_code,
    fmt="json",
    head="M",
)
# Write to the database


#########################
# Loop on all reporters #
#########################
# Download the last 5 years for one product, all reporters and all partners


########################
# Loop on all products #
########################
# Loop on products with a one hour pause every 10 000 records
