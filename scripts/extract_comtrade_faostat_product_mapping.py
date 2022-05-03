"""Script to keep only unique comtrade codes mapped to Faostat codes

The strategy could be to:
    - keep all rows that are not duplicated (corresponding to comtrade_code_unique1).

Then for the duplicated rows (corresponding to comtrade_code_duplicates_merge2):
    - exclude rows where comtrade codes don't have a description,
    - exclude rows where faostat codes don't have a description.

Then for the remaining rows (corresponding to comtrade_code_duplicates_merge3):
    - keep rows that are not duplicated,
    - exclude rows that are still duplicated.

Finally concats all unique values and sandwood products and save the mapping
"""

from biotrade.comtrade import comtrade
from biotrade.faostat import faostat
from pathlib import Path
import pandas as pd

# Comtrade-Faostat mapping table url
url = "http://datalab.review.fao.org/datalab/caliper/web/sites/default/files/2020-01/HS_FCL_mappings_2020-01-07.csv"
# Read url into a dataframe
df = pd.read_csv(url)
rename_dict = {"HS code": "comtrade_code", "FCL code": "faostat_code"}
# Select only columns needed
df = df.rename(columns=rename_dict)[rename_dict.values()]
df = df.astype({"comtrade_code": str})
# Consider only unique comtrade codes
comtrade_code_unique1 = df.drop_duplicates(subset="comtrade_code", keep=False)
# Select duplicate codes
comtrade_code_duplicates = df[df.comtrade_code.duplicated(keep=False)]
# Select product table of Comtrade db
comtrade_products = comtrade.db.select("product").rename(
    columns={"product_code": "comtrade_code"}
)
# Merge duplicate codes with product table of Comtrade db
comtrade_code_duplicates_merge1 = comtrade_code_duplicates.merge(
    comtrade_products, on="comtrade_code", how="left"
)
# Select faostat table
faostat_table = faostat.db.tables["crop_trade"]
# Select product names and codes from Faostat db
stmt = (
    faostat_table.select()
    .with_only_columns([faostat_table.c.product, faostat_table.c.product_code])
    .distinct(faostat_table.c.product, faostat_table.c.product_code)
)
faostat_products = pd.read_sql_query(stmt, faostat.db.engine).rename(
    columns={"product_code": "faostat_code"}
)
# Merge comtrade duplicates with products of Faostat db
comtrade_code_duplicates_merge2 = comtrade_code_duplicates_merge1.merge(
    faostat_products, on="faostat_code", how="left"
)
# Exclude rows which have comtrade and faostat product descriptions nan
comtrade_code_duplicates_merge3 = comtrade_code_duplicates_merge2.loc[
    ~comtrade_code_duplicates_merge2["product_description"].isna()
    & ~comtrade_code_duplicates_merge2["product"].isna()
]
# Keep track of duplicates discarded
comtrade_code_duplicates_disc1 = comtrade_code_duplicates_merge2[
    ~comtrade_code_duplicates_merge2.index.isin(comtrade_code_duplicates_merge3.index)
][["comtrade_code", "faostat_code"]]
# Consider only unique comtrade codes from the merges
comtrade_code_unique2 = comtrade_code_duplicates_merge3.drop_duplicates(
    subset="comtrade_code", keep=False
)[["comtrade_code", "faostat_code"]]
# Keep track of duplicates discarded
comtrade_code_duplicates_disc2 = comtrade_code_duplicates_merge3[
    comtrade_code_duplicates_merge3.comtrade_code.duplicated(keep=False)
][["comtrade_code", "faostat_code"]]
# Add sand wood products
sandwood_products_comtrade = [
    440710,
    440711,
    440712,
    440713,
    440714,
    440719,
    440721,
    440722,
    440723,
    440724,
    440725,
    440726,
    440727,
    440728,
    440729,
    440791,
    440792,
    440793,
    440794,
    440795,
    440796,
    440797,
    440799,
]
sandwood_products_faostat = [
    1632,
    1632,
    1632,
    1632,
    1632,
    1632,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
    1633,
]
comtrade_code_unique3 = pd.DataFrame()
comtrade_code_unique3["comtrade_code"] = sandwood_products_comtrade
comtrade_code_unique3["faostat_code"] = sandwood_products_faostat
# Concat unique comtrade codes and save the mapping
comtrade_code_unique_all = pd.concat(
    [comtrade_code_unique1, comtrade_code_unique2, comtrade_code_unique3],
    ignore_index=True,
)
comtrade_code_unique_all.to_csv(
    Path.cwd() / "biotrade" / "config_data" / "comtrade_faostat_product_mapping.csv",
    index=False,
)
# Concat duplicated comtrade codes discarded and save them
comtrade_code_discarded_all = pd.concat(
    [comtrade_code_duplicates_disc1, comtrade_code_duplicates_disc2], ignore_index=True,
)
comtrade_code_discarded_all.to_csv(
    Path.cwd() / "scripts" / "comtrade_faostat_product_mapping_discarded.csv",
    index=False,
)
