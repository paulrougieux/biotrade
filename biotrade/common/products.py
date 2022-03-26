#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the product mapping

Usage:

    >>> from biotrade.common.products import comtrade_faostat_mapping

"""
# Third party modules
import pandas
from biotrade import module_dir

config_data_dir = module_dir / "config_data"
comtrade_faostat_mapping = pandas.read_csv(
    config_data_dir / "comtrade_faostat_product_mapping.csv"
)
