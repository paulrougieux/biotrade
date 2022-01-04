---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.5
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

```python
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_by_country_groups
from plotnine import ggplot, aes, geom_line, facet_grid, labs
import plotnine
plotnine.options.figure_size = (12, 8)

# Database
db = faostat.db
```

<!-- #region -->
# Introduction


The purpose of this notebok is to explore faostat data using methods in the biotrade package.
<!-- #endregion -->

# By Continents

Select crop production and crop bilateral trade where products contain the word "soy" then aggregate soy production and trade by continents.

```python
soy_prod = db.select(table="crop_production", product = "soy")
soy_trade = db.select(table="crop_trade", product = "soy")
soy_prod_c = agg_by_country_groups(soy_prod, agg_reporter = 'continent')
soy_trade_c = agg_by_country_groups(soy_trade, agg_reporter = 'continent', agg_partner = 'continent')
soy_trade_c["value_million"] = soy_trade_c["value"] / 1e6
```

```python
for col in ["product", "element", "continent_reporter", "continent_partner", "unit"]:
    print(col, ":", soy_trade_c[col].unique())
display(soy_trade_c)
```

```python
(
    ggplot(soy_trade_c.query("element == 'import_quantity'"), 
           aes(x='period', y='value_million', color="product"))
    + geom_line()
    + facet_grid('continent_partner ~ continent_reporter + element', 
                scales='free_y')
    + labs(x='year', y='Import Quantity (million tonnes)')
)
```

```python

```
