"""The purpose of this script is to prepare a mapping table between FAOSTAT and Comtrade product codes.

Usage:

    ipython -i ~/repos/forobs/biotrade/scripts/mapping_table_comtrade_faostat_product_codes.py

This script:
- Load FAOSTAT product names and codes
- Load Comtrade product names and codes
- Load comtrade faostat mapping table
- Merge the 3 tables

"""

from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade.common.products import comtrade_faostat_mapping


# FAOSTAT product names in the production table
fp = faostat.db.select("forestry_production")
print(fp["element"].unique())
faostat_names = (
    fp[["product_code", "product"]]
    .value_counts()
    .reset_index()
    .drop(columns=(0))
    .rename(columns={"product_code": "faostat_code", "product": "faostat_name"})
)

# with pandas.option_context("display.max_rows", 100, "display.max_columns", 10):
#     print(faostat_names)

# Comtrade product names
comtrade_names = comtrade.products.hs.rename(
    columns={"product_code": "comtrade_code", "product_description": "comtrade_name"}
)

# Mapping between comtrade and faostat codes
cf_mapping_wood = comtrade_faostat_mapping.query(
    "comtrade_code.str.startswith('44')"
).copy()

######################
# Merge the 3 tables #
######################
product_map = cf_mapping_wood.merge(
    faostat_names, on="faostat_code", how="outer"
).merge(comtrade_names, on="comtrade_code", how="left")

# Check that aggregates of sawnwood products correspond to the sum of their parts
sawn_prod = fp.query("product_code in [1632, 1633, 1872] and element == 'production'")
index = ["reporter_code", "reporter", "product_code", "product", "year", "unit"]
sawn_prod_agg = sawn_prod.groupby(index)["value"].agg("sum").reset_index()
# Compare for Italy in 2020
sawn_prod_agg.query("reporter=='Italy' and year == 2020")

# Check that aggregates of fuel wood products correspond to the sum of their parts
# 1630                                      wood_charcoal
# 1693                                       wood_pellets
# 1694                                 other_agglomerates
# 1696                wood_pellets_and_other_agglomerates
# 1864                                          wood_fuel
fuel_prod = fp.query(
    "product_code in [1630 , 1693 , 1694 , 1696 , 1864] and element == 'production'"
)
index = ["reporter_code", "reporter", "product_code", "product", "year", "unit"]
fuel_prod_agg = fuel_prod.groupby(index)["value"].agg("sum").reset_index()
# Compare for Italy in 2020
fuel_prod_agg.query("reporter=='Italy' and year == 2020")


# Add faostat codes
