---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Quality check of main commodities

Goals:
- Check for each primary commodity the difference between aggregation country data and World country data reported by Faostat and Comtrade
- Plot results

```python
# Import libraries and dependencies
from biotrade.faostat.quality import (
    compare_crop_production_harvested_area_world_agg_country,
)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import plotly.graph_objects as go
```

```python
# Soybean, oil palm fruit, coffee green, cocoa beans, cattle,
product_code_list = [236, 254, 656, 661, 866]
# Faostat production dataframe
df = compare_crop_production_harvested_area_world_agg_country(product_code=product_code_list)
```

```python
# Plot with seaborn library
sns.set_style('darkgrid')
sns.set(rc={'figure.figsize':(14,8)})
ax = sns.lineplot(data=df, x ='year', y = 'qnt_diff', hue=df[["product","element", "unit"]].apply(tuple, axis=1),
                  legend='full', lw=3)
# Show year labels with multiple of 5
ax.xaxis.set_major_locator(ticker.MultipleLocator(5))
# Put legend top right corner
plt.legend(bbox_to_anchor=(1, 1))
plt.ylabel('Aggregation - World data')
plt.xlabel('Year')
plt.title("Faostat production data");
```

```python
# Extract list of unique tuples
list_lines = df[["product","element", "unit"]].apply(tuple, axis=1).unique().tolist()
# Extract color palette, the palette can be changed
list_colors = list(sns.color_palette(palette="tab10", n_colors=len(list_lines)).as_hex())
```

```python
# Plot interactively with plotly library
fig = go.Figure()
for line, color in zip(list_lines, list_colors):
    fig.add_trace(go.Scatter(x = df[(df['product']==line[0]) & (df['element']==line[1]) & (df['unit']==line[2])]['year'],
                             y = df[(df['product']==line[0]) & (df['element']==line[1]) & (df['unit']==line[2])]['qnt_diff'],
                             name = str(line),
                             line_color = color, 
                             fill=None))
fig.update_layout(xaxis_title="Year", yaxis_title="Aggregation - World data", title= "Faostat production data")

fig.show()
```
