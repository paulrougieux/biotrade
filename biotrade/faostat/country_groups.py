#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Usage:

    >>> from biotrade.faostat import faostat
    >>> print("List of EU countries:")
    >>> print(faostat.country_groups.eu_country_names)
    >>> print("\n\nData frame of country groups:")
    >>> print(faostat.country_groups.df)

"""

# Third party modules
import pandas


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
    def df(self):
        """Country groupings

        :return: A data frame of table faostat_country_groups.csv

            >>> from biotrade.faostat import faostat
            >>> df = faostat.country_groups.df

        """
        path = self.config_data_dir / "faostat_country_groups.csv"
        df = pandas.read_csv(path)
        # Convert NA values to empty strings so that there is no issue reading it from R
        df.fillna("", inplace=True)
        return df

    @property
    def eu_country_names(self):
        """
        EU country name list in the FAOSTAT data
        :return list of eu country names

            >>> from biotrade.faostat import faostat
            >>> eu_country_name_list = faostat.country_groups.eu_country_names
        """
        df = self.df
        return df[df["eu27"] == 1]["short_name"].tolist()

    @property
    def continents(self):
        """Country groupings by continents and subcontinents

        For example select from the crop trade table soy products and merge
        with the continents and subcontinents data, regarding both reporter
        and partner countries.

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db
        >>> df_soy = db.select(table="crop_trade", product = "soy")
        >>> df_continents = faostat.country_groups.continents
        >>> df_soy_merge = df_soy.merge(df_continents, how='left',
                left_on = 'reporter_code', right_on = 'faost_code')
        >>> df_soy_merge = df_soy_merge.merge(df_continents, how='left',
                left_on = 'partner_code', right_on = 'faost_code',
                suffixes=('_reporter','_partner'))
        """
        return self.df[["faost_code", "continent", "sub_continent"]]
