---
jupyter:
  jupytext:
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

# Detect relative and absolute time series change


Import dependencies

```python
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.common.time_series import relative_absolute_change, plot_relative_absolute_change
import matplotlib.pyplot as plt
```

Select soybeans export quantity with Brazil as reporter

```python tags=[]
soy_trade = faostat.db.select(table="crop_trade", reporter="Brazil", product="soy")
soybeans_export = soy_trade[(soy_trade["product"] == "soybeans") & (soy_trade["element"] == "export_quantity")]
```

Aggregate partners as Europe (eu) and rest of the World (row)

```python tags=[]
soybeans_export_agg = agg_trade_eu_row(soybeans_export)
```

Calculate the relative and absolute change of values in time (5 years as default)

```python
soybeans_exp_change = relative_absolute_change(soybeans_export_agg, last_value=False)
```

Print the dataframe

```python
soybeans_exp_change
```

Plot bars

```python
plot_relative_absolute_change(soybeans_exp_change)
```

Calculate the relative and absolute change of values with respect to fix year range 2000-2010

```python
soybeans_exp_change_range_2000_2010 = relative_absolute_change(soybeans_export_agg, last_value=False, year_range = [2000, 2010])
```

Plot bars

```python
plot_relative_absolute_change(soybeans_exp_change_range_2000_2010)
```

Print the dataframe

```python
soybeans_exp_change_range_2000_2010
```
