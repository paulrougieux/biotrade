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
from biotrade import module_dir

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
        >>> aut.hwp.production_wide

    """

    def __repr__(self):
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, parent):
        """Get the country name from this object's parent class."""
        self.country_name = parent.country_name
        self.faostat = parent.faostat

    @property
    def production(self):
        """FAOSTAT forestry production data"""
        df = self.faostat.forestry_production.copy()
        # Prepare shorter column names combination of product and element
        df = df.merge(PRODUCTS, on=["product", "product_code"], how="inner")
        df = df.merge(ELEMENTS, on=["element"], how="inner")
        df = df[
            df.element.isin(["production", "import_quantity", "export_quantity"])
        ].copy()
        df["prod_elem"] = df["product_short"] + "_" + df["element_short"]
        return df

    @property
    def production_wide(self):
        """Reshape FAOSTAT forestry production data to wide format

        >>> from biotrade.common.country import Country
        >>> aut = Country("Austria")
        >>> aut.hwp.production_wide

        For information only, equivalent data frame using a multi index pivot

        >>> prod_wide = (aut.hwp.production
        >>>              .pivot(index=["reporter", "year"], columns=["product", "element"], values="value")
        >>>             )
        """
        df = (
            self.production.pivot(
                index=["reporter", "year"], columns="prod_elem", values="value"
            )
            .reset_index()
            .rename_axis(columns=None)
        )
        return df

    def trade(self):
        """Reshape wood trade data to wide format"""
        df = self.faostat.forestry_trade.copy()
        return df
