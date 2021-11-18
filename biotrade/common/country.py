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

    For example display FAOSTAT forestry production data for one country:

        >>> from biotrade.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.faostat.forestry_production

    FAOSTAT crop trade for one country

        >>> from biotrade.common.country import Country
        >>> bra = Country("Brazil")
        >>> bra.faostat.crop_trade
        >>> bra.faostat.crop_trade_mirror

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
