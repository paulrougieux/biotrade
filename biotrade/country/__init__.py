#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC Biomass Project.
Unit D1 Bioeconomy.
"""

# Third party modules
import pandas

# Internal modules
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.faostat.convert import convert_to_eq_rwd_level_1


###############################################################################
class Country(object):
    """
    This object gives access to the data pertaining to one country.
    To access data for many countries, use the faostat and comtrade objects directly.

    For example display FAOSTAT forestry production and trade data for one country:

        >>> from biotrade.core.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.faostat_forestry_production
        >>> ukr.faostat_forestry_trade
        >>> ukr.faostat_forestry_trade_mirror
        >>> ukr.faostat_forestry_prod_trade_eqrwd
    """

    def __repr__(self):
        # TODO: add iso 3 code here
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, country_name):
        """Initialize a country with its name."""
        self.country_name = country_name

    @property
    def faostat_forestry_production(self):
        """FAOSTAT forestry production data for one reporter country"""
        return faostat.db.select(
            table="forestry_production", reporter=self.country_name
        )

    @property
    def faostat_forestry_trade(self):
        """FAOSTAT forestry bilateral trade data (trade matrix) for one
        reporter country and all its partner countries"""
        return faostat.db.select(table="forestry_trade", reporter=self.country_name)

    @property
    def faostat_forestry_trade_eu_row(self):
        """FAOSTAT forestry bilateral trade with partners aggregated by EU and Rest of the World"""
        return agg_trade_eu_row(self.faostat_forestry_trade, index_side="partner")

    @property
    def faostat_forestry_trade_mirror(self):
        """FAOSTAT forestry bilateral trade data (trade matrix) for alls
        reporter countries and one partner country"""
        return faostat.db.select(table="forestry_trade", partner=self.country_name)

    @property
    def faostat_forestry_trade_mirror_eu_row(self):
        """FAOSTAT mirror flows with reporters aggregated by EU and ROW"""
        df = agg_trade_eu_row(self.faostat_forestry_trade_mirror, index_side="reporter")
        # TODO: Rename column with a "mirror" suffix
        # df.rename(columns={
        # In the element column, exports become import and vice versa.
        return df

    @property
    def faostat_forestry_prod_trade_eqrwd(self):
        """FAOSTAT forestry production and trade data in volume equivalent roundwood

        Bilateral trade products are not the same as the production ones, they
        need to be aggregated using a mapping table defined in faostat.product.groups

        Load the data frame and show available elements:

            >>> from biotrade.core.country import Country
            >>> ukr = Country("Ukraine")
            >>> pt_eqrwd = ukr.faostat_forestry_prod_trade_eqrwd
            >>> pt_eqrwd.element.unique()

        """
        index = [
            "reporter_code",
            "reporter",
            "product_group",
            "element",
            "unit",
            "year",
        ]
        # Aggregate trade by production relevant product groups
        ft1_agg = (
            self.faostat_forestry_trade_eu_row.merge(
                faostat.products.groups, on="product"
            )
            .groupby(index)
            .agg(value=("value", sum))
            .reset_index()
            .rename(columns={"product_group": "product"})
        )
        # Convert to roundwood equivalent volumes #
        ft1eurow_eqr = convert_to_eq_rwd_level_1(ft1_agg)
        fp1_eqr = convert_to_eq_rwd_level_1(self.faostat_forestry_production).drop(
            columns={"element_code", "flag", "period", "product_code"}
        )
        # check that columns are identical
        set(fp1_eqr).symmetric_difference(ft1eurow_eqr)
        # concatenate
        df = pandas.concat([fp1_eqr, ft1eurow_eqr])
        return df
