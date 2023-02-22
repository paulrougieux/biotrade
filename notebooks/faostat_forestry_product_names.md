---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.4
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

```python
import seaborn
import pandas

from biotrade.faostat import faostat

```

# Introduction

The purpose of this notebook is to give an overview of the FAOSTAT forestry production dataset, in particular what products are in there. 


## Load data

```python
fp = faostat.db.select("forestry_production")
```

```python
for col in ['reporter', 'product_code', 'product', 'element', 'period', 'year', 'unit', 'flag']:
    print(f"{col}:", fp[col].unique())
```

```python
fp.columns
```

## List of FAOSTAT products

```python
# FAOSTAT product names in the production table
faostat_names = (
    fp[["product_code", "product"]]
    .value_counts()
    .reset_index()
    .drop(columns=(0))
    .rename(columns={"product_code": "faostat_code", "product": "faostat_name"})
    .sort_values("faostat_code")
)
```

```python
with pandas.option_context("display.max_rows", 100, "display.max_columns", 10):
    display(faostat_names)
```

```python

```
