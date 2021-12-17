#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Usage:

    >>> from biotrade.faostat import faostat
    >>> print(faostat.country_groups.eu_country_names)

"""

# Third party modules
import pandas


# List of EU countries names in the FAOSTAT data
EU_COUNTRY_NAMES = [
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "Germany",
    "France",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
]


class CountryGroups(object):
    """
    Comtrade product list, with additional information.
    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Directories #
        self.config_data_dir = self.parent.config_data_dir

    @property
    def eu_country_names(self):
        """EU country names in the FAOSTAT data"""
        return EU_COUNTRY_NAMES

    @property
    def continents(self):
        """Country groupings by continents and subcontinents

        :return: A data frame of table faostat_country_groups.csv

        For example select from the crop trade table soy products and merge
        with the continents and subcontinents data, regarding both reporter
        and partner countries.

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db
        >>> df_soy = db.select(table="crop_trade", product = "soy")
        >>> df_continents = faostat.country_groups.continents[
                ['faost_code','continent', 'sub_continent']]
        >>> df_soy_merge = df_soy.merge(df_continents, how='left',
                left_on = 'reporter_code', right_on = 'faost_code')
        >>> df_soy_merge = df_soy_merge.merge(df_continents, how='left',
                left_on = 'partner_code', right_on = 'faost_code',
                suffixes=('_reporter','_partner'))
        """
        path = self.config_data_dir / "faostat_country_groups.csv"
        df = pandas.read_csv(path)
        return df
