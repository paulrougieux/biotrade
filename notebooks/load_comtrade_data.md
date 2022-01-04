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
from biotrade.comtrade import comtrade
import pandas
```

# Load monthly trade data from UN Comtrade into the database
The goal is to load monthly bilateral trade data. Here we report the example of data api download and pump to database from 2016 to 2021 for some vegetable oil products (starting with code "15" and containing the products of "palm", "soy", "sun", "rape") at the 6 digit level into the Comtrade schema raw_comtrade table raw_comtrade.monthly.

Select products from api comtrade starting with code "15" and having 6 digit code

```python
p = comtrade.pump.get_parameter_list("classificationHS.json")
p15_6dig = p[(p['id'].str.startswith('15')) & (p['id'].str.len() == 6)]
```

Select code list containing palm, soy, sun and rape vegetable oils

```python
veg_oils = ['palm', 'soy', 'sun', 'rape']
veg_6dig_codes = p15_6dig[p15_6dig['text'].str.lower().str.contains('|'.join(veg_oils))].id.values.tolist()
```

Download monthly data from 2016 to 2021 and pump data to database. The function dowload_to_db_reporter_loop loops on reporter codes

```python
comtrade.pump.download_to_db_reporter_loop(
    table_name = 'monthly',
    start_year = 2016,
    end_year = 2021,
    product_code = veg_6dig_codes,
    frequency = "M",
)
```

# Load yearly trade data for HS2 from UN Comtrade into the database
The goal is to load yearly bilateral trade data. Here we report the example of data api download and pump to database from year 2000 to 2021 for products with 2 digit codes (from config_data/comtrade_hs_2d.csv) into the Comtrade schema raw_comtrade table raw_comtrade.yearly_hs2.

Select product codes from config_data/comtrade_hs_2d.csv and transform them into a list of strings

```python
path = comtrade.config_data_dir/ "comtrade_hs_2d.csv"
p_2dig_int = pandas.read_csv(path)['id'].tolist()
p_2dig = [str(int) for int in p_2dig_int]
```

Download yearly data from 2000 to 2021 and pump data to database. The function dowload_to_db_reporter_loop loops on reporter codes

```python
comtrade.pump.download_to_db_reporter_loop(
    table_name = 'yearly_hs2',
    start_year = 2000,
    end_year = 2021,
    product_code = p_2dig,
    frequency = "A",
)
```
