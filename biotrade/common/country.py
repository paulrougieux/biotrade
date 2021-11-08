#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC Biomass Project.
Unit D1 Bioeconomy.
"""


# Internal modules
from biotrade.faostat.country import FaostatCountry


class Country:
    """
    This object gives access to the data pertaining to one country.
    To access data for many countries, use the faostat and comtrade objects directly.

    For example display FAOSTAT forestry production and trade data for one country:

        >>> from biotrade.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.faostat.forestry_production
        >>> ukr.faostat.forestry_trade
        >>> ukr.faostat.forestry_trade_mirror
        >>> ukr.faostat.forestry_prod_trade_eqrwd
    """

    def __repr__(self):
        # TODO: add iso 3 code here
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, country_name):
        """Initialize a country with its name."""
        self.country_name = country_name

    @property
    def faostat(self):
        """FAOSTAT data for one country"""
        return FaostatCountry(self)
