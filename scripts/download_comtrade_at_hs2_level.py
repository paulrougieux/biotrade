#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

The purpose of this script is to download yearly Comtrade data at the HS2 level
for all reporter and all partner countries. The data is then stored into a
PostgreSQL database.

Drop the table in two different ways

    >>> from biotrade.comtrade import comtrade
    >>> comtrade.database.engine.execute("DROP TABLE raw_comtrade.yearly_hs2;")
    >>> comtrade.database.yearly_hs2.drop()

Alternative from bash and using the PostGreSQL client.
Create the database structure from the sql file in the `config_data` folder
This step can be skipped if the data is written the first time using pandas.to_sql().

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

# Internal modules
import datetime

# Third party modules
import logging
import pandas

# First party modules
from biotrade.comtrade import comtrade

# Get a logger object
logger = logging.getLogger("biotrade.comtrade")
# For the location of the log file, see logger.py


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
comtrade.database.append(wood_it, "yearly_hs2")


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


#########################
# Loop on all reporters #
#########################
# Download the last 5 years for one product, all reporters and all partners
RECORDS_DOWNLOADED = 0
PRODUCT_CODE = "44"
# Download wood products "44" data for all countries
# And store each data frame in a dictionary with country keys
# Upon download failure, give a message, wait 10 seconds
# then try to download again.
# In case of new failure, double the wait time until the download works again
wood = dict()
for reporter_code in reporters.id:
    reporter_name = reporters.text[reporters.id == reporter_code].to_string(index=False)
    download_successful = False
    sleep_time = 10
    # Try to download doubling sleep time until it succeeds
    while not download_successful:
        try:
            df = comtrade.pump.download(
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
            download_successful = True
        except Exception as e:
            logger.info("Failed to download %s \n %s", reporter_name, e)
            sleep_time *= 2
    # Store in a dictionary for debugging purposes
    # Remove this for the production code
    wood[reporter_code] = df
    # Store in the database store the message if it fails
    try:
        comtrade.database.append(df, "yearly_hs2")
    except Exception as e:
        logger.info("Failed to store %s in the database\n %s", reporter_name, e)
        sleep_time *= 2
    # Keep track of the country name and length of the data downloaded
    RECORDS_DOWNLOADED += len(wood[reporter_code])
    logger.info(
        "Downloaded %s records for %s (code %s).\n" + "%s records downloaded in total.",
        len(df),
        reporter_name,
        reporter_code,
        RECORDS_DOWNLOADED,
    )

# On the first attempt, the query timed out after 9 downloads
# leaving the dictionary with the following country codes:
# In : wood.keys()
# Out: dict_keys(['4', '8', '12', '20', '24', '660', '28', '32', '51'])

# On the second attempt, the query timed out after 19 attempts,
# leaving the dictionnary with the following coutnry codes:
# In : wood.keys()
# Out: dict_keys(['4', '8', '12', '20', '24', '660', '28', '32', '51',
# '533', '36', '40', '31', '44', '48', '50', '52', '112', '56'])


# Concatenate all data frames in the dictionary
wood_df = pandas.concat(wood)
print(wood_df.reporter.unique())
len(wood_df)

#############################################
# Loop on all products at the 2 digit level #
#############################################
# TODO: use the API connection key
# Loop on products with a one hour pause every 10 000 records
