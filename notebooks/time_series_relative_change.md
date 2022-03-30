---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.7
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Detect relative time series change


Import dependencies

```python
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.common.time_series import relative_change
import matplotlib.pyplot as plt
```

Select soybeans export quantity with Brazil as reporter

```python tags=[]
soy_trade = faostat.db.select(table="crop_trade", reporter="Brazil", product="soy")
soybeans_export = soy_trade[(soy_trade["product"] == "soybeans") & (soy_trade["element"] == "export_quantity")]
```

Aggregate partners as Europe (eu) and rest of the World (row)

```python
soybeans_export_agg = agg_trade_eu_row(soybeans_export)
```

Calculate the relative change of values in time (5 years as default)

```python
soybeans_exp_relative_change = relative_change(soybeans_export_agg, last_value=False)
```

Plot bars

```python
# Reshape dataframe for plot
df_reshape = soybeans_exp_relative_change.pivot_table(index='year', 
                        columns='partner', 
                        values='relative_change')
# Plot
ax = df_reshape.plot.bar(figsize=(20,10), fontsize= 16, color = ['tab:green','tab:orange'])
ax.set_xlabel("Time [y]", fontsize=20);
ax.set_ylabel("Relative change [%]", fontsize =20);
ax.legend(['EU-27','ROW'],fontsize=16);
ax.set_title('Export quantity change of soybeans from Brazil', fontsize = 20);
```

Calculate the relative change of values with respect to fix year range 2000-2010

```python
soybeans_exp_relative_change_range_2000_2010 = relative_change(soybeans_export_agg, last_value=False, year_range = [2000, 2010])
```

Plot bars

```python
# Reshape dataframe for plot
df_reshape = soybeans_exp_relative_change_range_2000_2010.pivot_table(index='year', 
                        columns='partner', 
                        values='relative_change')
# Plot
ax = df_reshape.plot.bar(figsize=(20,10), fontsize= 16, color = ['tab:green','tab:orange'])
ax.set_xlabel("Time [y]", fontsize=20);
ax.set_ylabel("Relative change [%]", fontsize =20);
ax.legend(['EU-27','ROW'],fontsize=16);
ax.set_title('Export quantity change of soybeans from Brazil with respect to average 2000-2010', fontsize = 20);
```
