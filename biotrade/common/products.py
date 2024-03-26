#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Get the product mapping

Usage:

    >>> from biotrade.common.products import comtrade_faostat_mapping
    >>> from biotrade.common.products import regulation_products

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

A Comtrade product code can only have 2,4 or 6 digits, the code blows checks
that this is the case. For example the code for coffee start with a zero
"090111" (it should not be "90111").

          09                  09 - Coffee, tea, mate and spices
        0901  0901 - Coffee, whether or not roasted or decaf...
      090111      090111 - Coffee; not roasted or decaffeinated

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

regulation_products = pandas.read_csv(
    config_data_dir / "regulation_products.csv",
    # Force Comtrade codes to be strings
    dtype={
        "regulation_code": "str",
        "comtrade_primary_code": "str",
        "hs_4d_code": "str",
        "hs_6d_code": "str",
    },
)

##################################
# Check Comtrade FAOSTAT mapping #
##################################
# A Comtrade product code can only have 2,4 or 6 digits, check this is the case.
nchar = comtrade_faostat_mapping["comtrade_code"].str.len().unique()
if not set(nchar).issubset({2, 4, 6}):
    comtrade_faostat_mapping["nchar"] = comtrade_faostat_mapping[
        "comtrade_code"
    ].str.len()
    msg = "A Comtrade product code can only have 2,4 or 6 digits. "
    msg += "The following codes do not comply:\n"
    msg += f"{comtrade_faostat_mapping.query('not nchar.isin([2,4,6])')}"
    raise ValueError(msg)

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
