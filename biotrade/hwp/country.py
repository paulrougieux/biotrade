#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC Biomass Project.
Unit D1 Bioeconomy.
"""


class HwpCountry:
    """
    This object gives access to the Harvested Wood Products data for one country.

        >>> from biotrade.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.hwp.production_wide

    """

    def __repr__(self):
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, parent):
        """Get the country name from this object's parent class."""
        self.country_name = parent.country_name
        self.faostat = parent.faostat

    @property
    def production_wide(self):
        """Reshape wood production data to wide format

        >>> from biotrade.common.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.hwp.production_wide

        """
        df = self.faostat.forestry_production

        return df

    def trade_wide(df):
        """Reshape wood trade data to wide format"""
