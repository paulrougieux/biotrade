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

http://datalab.review.fao.org/datalab/caliper/web/sites/default/files/2020-01/FCL_HS_mappings_2020-01-07.csv
which contains FCL codes, labels and the correspondence with Comtrade HS codes.

http://datalab.review.fao.org/datalab/caliper/web/sites/default/files/2020-01/HS_FCL_mappings_2020-01-07.csv
which contains Comtrade HS codes, Comtrade HS labels and the correspondence with FCL.

comtrade_faostat_mapping was made from the second table, HS_FCL mapping.
"""
# Third party modules
import pandas
from biotrade import module_dir

config_data_dir = module_dir / "config_data"
comtrade_faostat_mapping = pandas.read_csv(
    config_data_dir / "comtrade_faostat_product_mapping.csv"
)
