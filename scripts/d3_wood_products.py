"""
Script to extract data for Timba model (Julia Tandetzki)
"""

from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade import data_dir
from pathlib import Path
import numpy as np

comtrade_list_products = [
    "540310",
    "540331",
    "540332",
    "540333",
    "540341",
    "540342",
    "550410",
]
faostat_list_products = [
    1864,
    1866,
    1867,
    1871,
    1600,
    1632,
    1633,
    1634,
    1873,
    1640,
    1697,
    1874,
    1650,
    1685,
    1656,
    1667,
    1876,
    1671,
    1674,
    1675,
]
keep_cols = [
    "reporter_code",
    "partner_code",
    "year",
    "product_code",
    "element",
    "value",
    "unit",
]
path = Path(data_dir) / "d3"
df = comtrade.db.select(
    table="yearly",
    product_code=comtrade_list_products,
    period_start=2022,
    period_end=2024,
)
# Change units in tons and 1000 USD
df["net_weight"] = df["net_weight"] / 1000
df["trade_value"] = df["trade_value"] / 1000
# Use iso codes
df["reporter_code"] = df["reporter_iso"]
df["partner_code"] = df["partner_iso"]
# Melt columns
cols_to_melt = ["net_weight", "trade_value"]
df = df.melt(
    id_vars=[c for c in df.columns if c not in cols_to_melt],
    value_vars=cols_to_melt,
    var_name="element",
    value_name="value",
).assign(unit=lambda x: np.where(x["element"] == "net_weight", "t", "1000 USD"))
df["element"] = df["flow"] + "_" + df["element"]
df[keep_cols].to_csv(path / "comtrade_54_55.csv", index=False)
df = faostat.db.select(
    table="forestry_production",
    product_code=faostat_list_products,
    period_start=2022,
    period_end=2024,
)
df_country = faostat.country_groups.df[["faost_code", "iso3_code"]]
df = df.merge(df_country, how="inner", left_on="reporter_code", right_on="faost_code")
df["reporter_code"] = df["iso3_code"]
keep_cols.remove("partner_code")
df[keep_cols].to_csv(path / "faostast_forestry_production.csv", index=False)
