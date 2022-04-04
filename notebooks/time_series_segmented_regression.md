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

# Detect segments of a time series through piecewise regression

Here we report some examples of the application of segmeted regression implemented into the biotrade package

```python
# Import dependencies
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.common.time_series import segmented_regression
import matplotlib.pyplot as plt
```

### Db: Faostat, Reporter: Brazil, Partner: EU, Product: soybeans (236), Flow: export quantity

```python
# Select soybean trade product with Brazil as reporter
soybean_trade = faostat.db.select(
    table="crop_trade", reporter="Brazil", product_code=236
)
# Aggregate partners as Europe (eu) and rest of the World (row)
soybean_trade_agg = agg_trade_eu_row(soybean_trade)
# Select export quantity and eu as partner
soybean_exp_agg_eu = soybean_trade_agg[(soybean_trade_agg["element"] == "export_quantity") & (soybean_trade_agg["partner"] == "eu")]
# Use as objective function the residual sum of squares (RSS)
soybean_exp_eu_regression1 = segmented_regression(
    soybean_exp_agg_eu, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
soybean_exp_eu_regression2 = segmented_regression(
    soybean_exp_agg_eu, plot=True, last_value=False, function="R2"
)
```

```python
# print dataframe
soybean_exp_eu_regression1
```

```python
# print dataframe
soybean_exp_eu_regression2
```

<!-- #region tags=[] -->
### Db: Faostat, Reporter: Brazil, Partner: ROW, Product: soybeans (236), Flow: export quantity
<!-- #endregion -->

```python
# Select export quantity and row as partner
soybean_exp_agg_row = soybean_trade_agg[(soybean_trade_agg["element"] == "export_quantity") & (soybean_trade_agg["partner"] == "row")]
# Use as objective function the residual sum of squares (RSS)
soybean_exp_row_regression1 = segmented_regression(
    soybean_exp_agg_row, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
soybean_exp_row_regression2 = segmented_regression(
    soybean_exp_agg_row, plot=True, last_value=False, function="R2"
)
```

### Db: Faostat, Reporter: Malaysia, Product: palm oil (257), Flow: production

```python
# Select palm oil product with Maylesia as reporter
palm_oil_data = faostat.db.select(
    table="crop_production", reporter="Malaysia", product_code=257
)
# Select production quantity from 1986
palm_oil_prod = palm_oil_data[(palm_oil_data["element"] == "production") & (palm_oil_data["year"] > 1986)]
# Use as objective function the residual sum of squares (RSS)
palm_oil_prod_regression1 = segmented_regression(
    palm_oil_prod, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
palm_oil_prod_regression2 = segmented_regression(
    palm_oil_prod, plot=True, last_value=False, function="R2"
)
```

### Db: Faostat, Reporter: Cameroon, Partner: EU, Product: cocoabeans (661), Flow: export quantity

```python
# Select cocoabean trade products with Cameroon as reporter
cocoabean_trade = faostat.db.select(
    table="crop_trade", reporter="Cameroon", product_code=661
)
# Aggregate partners as Europe (eu) and rest of the World (row)
cocoabean_trade_agg = agg_trade_eu_row(cocoabean_trade)
# Select export quantity and eu as partner
cocoabean_exp_agg_eu = cocoabean_trade_agg[(cocoabean_trade_agg["element"] == "export_quantity") & (cocoabean_trade_agg["partner"] == "eu")]
# Use as objective function the residual sum of squares (RSS)
cocoabean_exp_eu_regression1 = segmented_regression(
    cocoabean_exp_agg_eu, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
cocoabean_exp_eu_regression2 = segmented_regression(
    cocoabean_exp_agg_eu, plot=True, last_value=False, function="R2"
)
```

### Db: Faostat, Reporter: Cameroon, Partner: Row, Product: cocoabeans (661), Flow: export quantity

```python
# Select export quantity and row as partner
cocoabean_exp_agg_row = cocoabean_trade_agg[(cocoabean_trade_agg["element"] == "export_quantity") & (cocoabean_trade_agg["partner"] == "row")]
# Use as objective function the residual sum of squares (RSS)
cocoabean_exp_row_regression1 = segmented_regression(
    cocoabean_exp_agg_row, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
cocoabean_exp_row_regression2 = segmented_regression(
    cocoabean_exp_agg_row, plot=True, last_value=False, function="R2"
)
```

### Db: Faostat, Reporter: Brazil, Product: coffee green (656), Flow: area harvested

```python
# Select coffee green product with Brazil as reporter
coffe_green_data = faostat.db.select(
    table="crop_production", reporter="Brazil", product_code=656
)
# Select area harvested quantity from 1986
coffe_green_areah = coffe_green_data[(coffe_green_data["element"] == "area_harvested") & (coffe_green_data["year"] > 1986)]
# Use as objective function the residual sum of squares (RSS)
coffe_green_areah_regression1 = segmented_regression(
    coffe_green_areah, plot=True, last_value=False, function="RSS"
)
# Use as objective function the coefficient of determination (R2)
coffe_green_areah_regression2 = segmented_regression(
    coffe_green_areah, plot=True, last_value=False, function="R2"
)

```

```python

```
