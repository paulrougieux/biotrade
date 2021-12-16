---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.4
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
from biotrade.faostat import faostat
```

<!-- #region -->
# Introduction


The purpose of this notebok is to explore faostat data using methods in the biotrade package.
<!-- #endregion -->

# By Continents

Aggregate soy production and trade by continents.

```python
 # Select crop production where products contain the word "soy"
db = faostat.db
soy_prod = db.select(table="crop_production", product = "soy")
#Select crop trade where products contain the word "soy"
soy_trade = db.select(table="crop_trade", product = "soy")
```

```python
# TODO: add method that aggregate the production and trade datasets by continents.
```
