#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC Biomass Project.
Unit D1 Bioeconomy.
"""

# Third party modules
import pandas

# Internal modules
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.faostat.convert import convert_to_eq_rwd_level_1


class FaostatCountry:
    """
    This object gives access to the FAOSTAT data for one country.
    To access data for many countries, use the faostat object directly.

    Display FAOSTAT forestry production and trade data for one country:

        >>> from biotrade.common.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.faostat.forestry_production
        >>> ukr.faostat.forestry_trade
        >>> ukr.faostat.forestry_trade_mirror
        >>> ukr.faostat.forestry_prod_trade_eqrwd

    Display FAOSTAT crop production and trade data for one country:

        >>> from biotrade.common.country import Country
        >>> bra = Country("Brazil")
        >>> bra.faostat.crop_production
        >>> bra.faostat.crop_trade
        >>> bra.faostat.crop_trade_mirror

    Note: crop trade selection has the longest query time at 6 seconds.

        %timeit bra.faostat.crop_trade
        6.06 s ± 206 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
    """

    def __repr__(self):
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, parent):
        """Get the country name from this object's parent class."""
        self.country_name = parent.country_name

    @property
    def forestry_production(self):
        """FAOSTAT forestry production data for one reporter country"""
        return faostat.db.select(
            table="forestry_production", reporter=self.country_name
        )

    @property
    def forestry_trade(self):
        """FAOSTAT forestry bilateral trade data (trade matrix) for one
        reporter country and all its partner countries"""
        df = faostat.db.select(table="forestry_trade", reporter=self.country_name)
        return df

    @property
    def forestry_trade_eu_row(self):
        """FAOSTAT forestry bilateral trade with partners aggregated by EU and Rest of the World"""
        return agg_trade_eu_row(self.forestry_trade, grouping_side="partner")

    @property
    def forestry_trade_mirror(self):
        """FAOSTAT forestry bilateral trade data (trade matrix) for alls
        reporter countries and one partner country"""
        return faostat.db.select(table="forestry_trade", partner=self.country_name)

    @property
    def forestry_trade_mirror_eu_row(self):
        """FAOSTAT mirror flows with reporters aggregated by EU and ROW"""
        df = agg_trade_eu_row(self.forestry_trade_mirror, grouping_side="reporter")
        # TODO: Rename column with a "mirror" suffix
        # df.rename(columns={
        # In the element column, exports become import and vice versa.
        return df

    @property
    def forestry_prod_trade_eqrwd(self):
        """FAOSTAT forestry production and trade data in volume equivalent roundwood

        Bilateral trade products are not the same as the production ones, they
        need to be aggregated using a mapping table defined in faostat.product.groups

        Load the data frame and show available elements:

            >>> from biotrade.common.country import Country
            >>> ukr = Country("Ukraine")
            >>> pt_eqrwd = ukr.faostat.forestry_prod_trade_eqrwd
            >>> print(pt_eqrwd.element.unique())
            >>> print(pt_eqrwd)

        """
        index = [
            "reporter_code",
            "reporter",
            "product_level_1",
            "element",
            "unit",
            "year",
        ]
        # Aggregate trade by production relevant product groups
        ft1_agg = (
            self.forestry_trade_eu_row.merge(
                faostat.products.forestry_trade_groups, on="product"
            )
            .groupby(index)
            .agg(value=("value", sum))
            .reset_index()
            .rename(columns={"product_level_1": "product"})
        )
        # Convert to roundwood equivalent volumes #
        ft1eurow_eqr = convert_to_eq_rwd_level_1(ft1_agg)
        fp1_eqr = convert_to_eq_rwd_level_1(self.forestry_production).drop(
            columns={"element_code", "flag", "period", "product_code"}
        )
        # check that columns are identical
        set(fp1_eqr).symmetric_difference(ft1eurow_eqr)
        # concatenate
        df = pandas.concat([fp1_eqr, ft1eurow_eqr])
        df = df.reset_index()
        return df

    @property
    def crop_production(self):
        """FAOSTAT crop production data for one reporter country"""
        return faostat.db.select(table="crop_production", reporter=self.country_name)

    @property
    def crop_trade(self):
        """FAOSTAT crop bilateral trade data (trade matrix) for one
        reporter country and all its partner countries"""
        df = faostat.db.select(table="crop_trade", reporter=self.country_name)
        return df

    @property
    def crop_trade_eu_row(self):
        """FAOSTAT crop bilateral trade with partners aggregated by EU and Rest of the World"""
        return agg_trade_eu_row(self.crop_trade, grouping_side="partner")

    @property
    def crop_trade_mirror(self):
        """FAOSTAT crop bilateral trade data (trade matrix) for alls
        reporter countries and one partner country"""
        return faostat.db.select(table="crop_trade", partner=self.country_name)
