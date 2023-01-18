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

from biotrade.common.compare import merge_faostat_comtrade


```

# Introduction


## Load data

```python
palm_comp = merge_faostat_comtrade(faostat_table="crop_trade",
                                   comtrade_table="yearly",
                                   faostat_code = [257], strict=False)
```

# Plot comparison

For example plot trade flows betwwen Indonesia and the Netherlands according to the 2 data sources.

```python
country_pair = ['Indonesia', 'Netherlands']
p_ind_net = palm_comp.query("reporter.isin(@country_pair) and partner.isin(@country_pair)")
p_ind_net
```

## Indonesia to Netherlands

```python
df1 = p_ind_net.query("reporter=='Netherlands' & partner =='Indonesia' & element=='import_quantity'")
p = seaborn.lineplot(x="year", y="value", style="source", data=df1)
p.set(ylabel = "Reported imported quantities", title = "Palm oil export from Indonesia to Netherlands (reported by Netherlands)")
p.figure.set_figwidth(15)
#p.figure.set_figheight(10)
```

```python
df2 = p_ind_net.query("reporter=='Indonesia' & partner =='Netherlands' & element=='export_quantity'")
p = seaborn.lineplot(x="year", y="value", style="source", data=df2)
p.set(ylabel = "Reported export quantities", title = "Palm oil export from Indonesia to Netherlands (reported by indonesia)")
p.figure.set_figwidth(15)
#p.figure.set_figheight(10)
```

```python

```
