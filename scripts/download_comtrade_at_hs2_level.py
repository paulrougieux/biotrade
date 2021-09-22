#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

The purpose of this script is to download yearly Comtrade data at the HS2 level
for all reporter and all partner countries. The data is then stored into a
PostgreSQL database.

Run this script at the command line with:

    ipython -i ~/repos/biotrade/scripts/download_comtrade_at_hs2_level.py

The main variables along which the download will be spread are:

- product
- reporter country
- partner country (all)
- year

The Comtrade API limits call complexity to a few products, a few years and a
few partners. To comply to API restrictions, the following script makes a
separate call for each products and each reporter country combination. Each
query loads 5 years of data for all partner countries.

There follows some database manipulation steps. Which have been useful at the
beginning of this project. Drop the table in two different ways

    >>> from biotrade.comtrade import comtrade
    >>> comtrade.database.engine.execute("DROP TABLE raw_comtrade.yearly_hs2;")
    >>> comtrade.database.yearly_hs2.drop()

Alternative from bash and using the PostGreSQL client.
Create the database structure from the sql file in the `config_data` folder
This step can be skipped if the data is written the first time using pandas.to_sql().
This step is now replaced by a structure defined in biotrade/database.py

    psql -d biotrade -h localhost -U rdb -f ~/rp/biotrade/biotrade/config_data/comtrade.sql

Connect to the `biotrade` database using the PostgreSQl client

    psql -d biotrade -h localhost -U rdb

Sample query to see the reporter countries

    select distinct(reporter) from raw_comtrade.yearly_hs2;

List schemas, and list tables in the raw_comext schema

    \dn
    \dt raw_comtrade.*;

Generate SQL Alchemy metadata from the table structure

    sqlacodegen --schema raw_comtrade --tables yearly_hs2 postgresql://rdb@localhost/biotrade
"""

# Third party modules
import logging

# First party modules
from biotrade.comtrade import comtrade

# Get a logger object
# For the location of the log file, see logger.py
logger = logging.getLogger("biotrade.comtrade")


##########################################
# Load metadata on product and countries #
##########################################

# Keep only bioeconomy related products at the 2 digit level
hs2d = comtrade.products.hs2d
hs2d_bio = hs2d[hs2d.bioeconomy == 1]
print(hs2d_bio.text)

# List of reporter countries
reporters = comtrade.countries.reporters
print(reporters)

#############################################
# Loop on all products at the 2 digit level #
#############################################
# TODO: use the API connection key
# Loop on products with a one hour pause every 10 000 records
for product_code in hs2d_bio.id:
    print(product_code)
    comtrade.pump.loop_on_reporters(product_code=product_code, table_name="yearly_hs2")


######################################################
# Development example of a single query for one step #
######################################################
# # Query parameters
# # Download the last 5 years for one product, one reporter and all its partners
# YEAR = datetime.datetime.today().year
# # Convert each element of the list to a string
# YEARS = [str(YEAR - i) for i in range(1, 6)]
# YEARS = ",".join(YEARS)
# PRODUCT_CODE = "44"
# REPORTER_CODE = "381"  # Italy
# # One call to the download function
# comtrade_data_it = comtrade.pump.download(
#     max="10000",
#     type="C",
#     freq="A",
#     px="HS",
#     ps=YEARS,
#     r=REPORTER_CODE,
#     p="all",
#     rg="all",
#     cc=PRODUCT_CODE,
#     fmt="json",
#     head="M",
# )
# # Write to the database
# comtrade.database.append(comtrade_data_it, "yearly_hs2")

# After adding a UNIQUE constraint, PostGreSQL returns the following error when
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


##################################################################
# Development example of a loop on all reporters for one product #
##################################################################
# comtrade.pump.loop_on_reporters(product_code="44", table_name="yearly_hs2")

# On the first attempt, the query timed out after 9 downloads
# leaving the dictionary with the following country codes:
# In : comtrade_data.keys()
# Out: dict_keys(['4', '8', '12', '20', '24', '660', '28', '32', '51'])

# On the second attempt, the query timed out after 19 attempts,
# leaving the dictionnary with the following coutnry codes:
# In : comtrade_data.keys()
# Out: dict_keys(['4', '8', '12', '20', '24', '660', '28', '32', '51',
# '533', '36', '40', '31', '44', '48', '50', '52', '112', '56'])
