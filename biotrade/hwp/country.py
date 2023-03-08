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
import numpy as np

# Internal modules
from biotrade import module_dir
from biotrade.faostat.aggregate import agg_trade_eu_row

PRODUCTS = pandas.read_csv(
    module_dir / "config_data/faostat_forestry_production_short_names.csv"
)
SELECTED_PRODUCTS = [
    "ir_c",
    "ir_nc",
    "pp",
    "sw",
    "sw_c",
    "sw_nc",
    "wp",
    "fw_c",
    "fw_nc",
    "wbp",
    "wpp",
]
PRODUCTS = PRODUCTS[PRODUCTS.product_short.isin(SELECTED_PRODUCTS)].copy()

ELEMENTS = pandas.DataFrame(
    {
        "element": [
            "production",
            "import_quantity",
            "import_value",
            "export_quantity",
            "export_value",
        ],
        "element_short": ["prod", "imp", "imp_usd", "exp", "exp_usd"],
    }
)


class HwpCountry:
    """
    This object gives access to the Harvested Wood Products data for one country.

        >>> from biotrade.common.country import Country
        >>> aut = Country("Austria")
        >>> aut.hwp.production

    """

    def __repr__(self):
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, parent):
        """Get the country name from this object's parent class."""
        self.country_name = parent.country_name
        self.faostat_country = parent.faostat
        self.products = PRODUCTS
        self.selected_products = SELECTED_PRODUCTS
        self.elements = ELEMENTS

    @property
    def production(self):
        """FAOSTAT forestry production data for the selected products

        >>> from biotrade.common.country import Country
        >>> aut = Country("Austria")
        >>> aut.hwp.production

        Reshape to wide format using short column names

        >>> prod_wide = (aut.hwp.production
        >>>              .pivot(index=["reporter", "year"],
        >>>                     columns=["prod_elem"],
        >>>                     values="value")
        >>>             )

        """
        df = self.faostat_country.forestry_production.copy()
        # Prepare shorter column names combination of product and element
        df = df.merge(PRODUCTS, on=["product", "product_code"], how="inner")
        df = df.merge(ELEMENTS, on=["element"], how="inner")
        df = df[
            df.element.isin(["production", "import_quantity", "export_quantity"])
        ].copy()
        df["prod_elem"] = df["product_short"] + "_" + df["element_short"]
        df = df.drop(columns=["element_short"])
        return df

    @property
    def trade(self):
        """Bilateral forestry trade data from FAOSTAT

        >>> from biotrade.common.country import Country
        >>> aut = Country("Austria")
        >>> aut.hwp.trade

        """
        df = self.faostat_country.forestry_trade.copy()
        return df

    @property
    def trade_eu_row(self, index_side="partner"):
        """FAOSTAT forestry trade data aggregated for EU and Non EU partners

        >>> from biotrade.common.country import Country
        >>> aut = Country("Austria")
        >>> aut.hwp.trade_eu_row

        >>> from biotrade.common.country import Country
        >>> bel = Country("Belgium")
        >>> bel.hwp.trade_eu_row

        """
        df = agg_trade_eu_row(self.trade, grouping_side=index_side)
        df = df.merge(PRODUCTS, on=["product", "product_code"], how="inner")
        return df

    @property
    def production_estimated_century(self):
        """Production estimated from 1901 to FAOSTAT start year"""
        df = self.production
        base_year = df["year"].min()
        df_base = df[df["year"] == base_year]
        df_past = pandas.concat([df_base] * 60, ignore_index=True)
        # Generate a list of number for each product group
        df_past["n"] = 1
        df_past["n"] = df_past.groupby(["prod_elem"])["n"].cumsum()
        # Use the IPCC estimation approach
        df_past = df_past.rename(columns={"value": "base_value"})
        df_past["value"] = df_past["base_value"] * np.exp(0.0151 * -df_past["n"])
        df_past = df_past.sort_values(["product", "year"])
        # TODO: finish the implementation of this method by correcting years
