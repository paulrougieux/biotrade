#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the product mapping

Usage:

    >>> from biotrade.common.products import comtrade_faostat_mapping

Source  http://datalab.review.fao.org/datalab/caliper/web/classification-page/41
FAOSTAT Commodity List (FCL) "Download mappings" with 2 csv tables:

- http://datalab.review.fao.org/datalab/caliper/web/sites/default/files/2020-01/FCL_HS_mappings_2020-01-07.csv
  which contains FCL codes, labels and the correspondence with Comtrade HS codes.

- http://datalab.review.fao.org/datalab/caliper/web/sites/default/files/2020-01/HS_FCL_mappings_2020-01-07.csv
  which contains Comtrade HS codes, Comtrade HS labels and the correspondence with FCL.

comtrade_faostat_mapping was made from the second table, HS_FCL mapping.


Only the lowest level of FAOSTAT product codes should be entered in this table.
Groups such as the faostat product sawnwood which is an aggregate of sawnwood
non coniferous and sawnwood coniferous should be handled at another level and
only the lowest level product codes of sawnwood non coniferous and sawnwood
coniferous should be present in the mapping table.

"""
# Third party modules
import pandas
from biotrade import module_dir

config_data_dir = module_dir / "config_data"
# Note the variable name is lower case as suggested in PEP 8
# https://peps.python.org/pep-0008/#function-and-variable-names
comtrade_faostat_mapping = pandas.read_csv(
    config_data_dir / "comtrade_faostat_product_mapping.csv",
    # Force Comtrade to be a string
    dtype={
        "comtrade_code": "str",
        # FAOSTAT code remains an integer
        "faostat_code": "int",
    },
)

# Check duplicates
# One comtrade code should always be associated to one and only one faostat code
# A faostat code can be associated to many comtrade codes
dup_codes = comtrade_faostat_mapping["comtrade_code"].duplicated(keep=False)
if any(dup_codes):
    msg = "Comtrade codes are not unique "
    msg += "in the comtrade to faostat mapping table.\n"
    msg += "The following duplicates are present:\n"
    msg += f"{comtrade_faostat_mapping.loc[dup_codes]}"
    raise ValueError(msg)
