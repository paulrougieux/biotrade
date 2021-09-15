#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

The purpose of this script is to download yearly Comtrade data at the HS2 level
for all reporter and all partner countries. The data is then stored into a
PostgreSQL database.


Create the database structure from the sql file in the `config_data` folder

    psql -d biotrade -h localhost -U rdb -f ~/rp/biotrade/biotrade/config_data/comtrade.sql

Connect to the `biotrade` database using the PostgreSQl client

    psql -d biotrade -h localhost -U rdb

List schemas, and list tables in the raw_comext schema

    \dn
    \dt raw_comtrade.*;
"""

# Internal modules
import datetime

# Third party modules
import logging

# First party modules
from biotrade.comtrade import comtrade

logger = logging.getLogger("biotrade.comtrade")

##########################################
# Load metadata on product and countries #
##########################################

# Keep only bioeconomy related products at the 2 digit level
hs2d = comtrade.products.hs2d
hs2d_bio = hs2d[hs2d.bioeconomy == 1]
print(hs2d_bio)

# List of reporter countries
reporters = comtrade.countries.reporters
print(reporters)

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
PRODUCT_CODE = "44"
REPORTER_CODE = "381"  # Italy
wood_it = comtrade.pump.download(
    max="10000",
    type="C",
    freq="A",
    px="HS",
    ps=YEARS,
    r=REPORTER_CODE,
    p="all",
    rg="all",
    cc=PRODUCT_CODE,
    fmt="json",
    head="M",
)
# Write to the database
comtrade.database.append_to_db(wood_it, "yearly_hs2")


# After adding the UNIQUE constraint, PostGreSQL returns the following error when
# I try to insert the same content twice:
#
#     IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value
#     violates unique constraint
#     "yearly_hs2_period_trade_flow_code_reporter_code_partner_cod_key"
#     DETAIL:  Key (period, trade_flow_code, reporter_code, partner_code,
#     commodity_code, qty_unit_code, flag)=(2017, 1, 381, 0, 44, 1, 4) already
#     exists.


# Bash command to export the table structure after logging in as the rdb user
# sudo -i -u rdb
# pg_dump -t 'raw_comtrade.yearly_hs2' --schema-only biotrade  >> /tmp/comtrade_yearly.sql


# TODO add the combination of codes and period as a unique constraint
# UNIQUE (period, trade_flow_code, reporter_code, partner_code,
#         commodity_code, qty_unit_code, flag)
# https://www.postgresql.org/docs/9.4/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINT


#########################
# Loop on all reporters #
#########################
# Download the last 5 years for one product, all reporters and all partners
RECORDS_DOWNLOADED = 0
PRODUCT_CODE = "44"
# Download wood products "44" data for all countries
wood = dict()
for reporter_code in reporters.id:
    wood[reporter_code] = comtrade.pump.download(
        max="10000",
        type="C",
        freq="A",
        px="HS",
        ps=YEARS,
        r=reporter_code,
        p="all",
        rg="all",
        cc=PRODUCT_CODE,
        fmt="json",
        head="M",
    )
    RECORDS_DOWNLOADED += len(wood[reporter_code])
    logger.info(
        f"Downloaded {len(wood[reporter_code])} records for "
        + f"{reporters.text[reporters.id == reporter_code].to_string(index=False)} "
        + f"(code {reporter_code}).\n"
        + f"{RECORDS_DOWNLOADED} records downloaded in total."
    )

#############################################
# Loop on all products at the 2 digit level #
#############################################

# Loop on products with a one hour pause every 10 000 records
