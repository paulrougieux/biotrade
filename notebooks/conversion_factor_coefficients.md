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

# Plot of extraction rate and waste of supply statistics
Import libraries

```python
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from itertools import chain
```

Load coefficients

```python
df = pd.read_csv(
    Path.cwd().parents[0]
    / "biotrade"
    / "config_data"
    / "faostat_agri_conversion_factors.csv",
    encoding="utf-8",
)
```

Major product code list and filter dataframe

```python
major_product_codes = pd.read_csv(
    Path.cwd().parents[0]
    / "biotrade"
    / "config_data"
    / "crop_commodity_tree_faostat.csv",
    encoding="utf-8",
)[['parent_code', 'child_code']].values.tolist()
# Select unique values
major_product_codes = list(chain(*major_product_codes))
major_product_codes = list(set(major_product_codes))
# Select only major products for plots
df = df[df['fao_product_code'].isin(major_product_codes)]
# Group per product code
grouped = df.groupby('fao_product_code')
```

Plot scatter values

```python
list_coefficients = [['extraction_rate', 'b'], ['waste_of_supply','r']]
for coeff in list_coefficients:
  plt.figure()
  for group in grouped:
    if not group[1][coeff[0]].dropna().empty:
      plt.scatter(group[1][coeff[0]],group[1]['fao_product'], facecolors='none', edgecolors=coeff[1])
  plt.xlabel(coeff[0])
  plt.ylabel('product')
  plt.subplots_adjust(left=0.3)
  plt.show()
```

Plot histrograms of extraction rate and waste of supply against average mean for each product

```python
for coeff in list_coefficients:
  for group in grouped:
    if not group[1][coeff[0]].dropna().empty:
      plt.figure()
      plt.hist(group[1][coeff[0]].dropna(), color = coeff[1], label = 'data')
      plt.axvline(group[1][coeff[0]].dropna().mean(), color='g', linestyle='solid', linewidth=2, label = 'average mean')
      plt.title(f"{group[1]['fao_product'].unique()[0]} ({int(group[1]['fao_product_code'].unique()[0])})")
      plt.xlabel(coeff[0])
      plt.ylabel('frequency')
      plt.legend()
      plt.show()
```
