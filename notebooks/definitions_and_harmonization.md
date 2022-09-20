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

# Introduction

The purpose of this notebook is to:
1. Define variables used in the different data sources and illustrate differences.
2. Explain how the biotrade package harmonises variables names before they are storred into database tables.


# Definitions

## Area, yield and production

The faostat crop production data provides area, yield and production data, which have their respective units visible in :

    cp = faostat.db.select(table="crop_production", reporter=["Portugal", "Estonia"])
    cp[["element", "unit"]].drop_duplicates()

| element                       | unit      |
|-------------------------------|-----------|
| area_harvested                | ha        |
| yield                         | hg/ha     |
| production                    | tonnes    |
| stocks                        | No        |
| stocks                        | Head      |
| stocks                        | 1000 Head |
| laying                        | 1000 Head |
| production                    | 1000 No   |
| producing_animals_slaughtered | Head      |
| yield                         | hg/An     |
| yield                         | hg        |
| yield_carcass_weight          | hg/An     |
| yield_carcass_weight          | 0.1g/An   |
| producing_animals_slaughtered | 1000 Head |
| yield                         | 100mg/An  |



## Commodities and products

The meaning of the words "commodity" and "product" change depending on the context. Sometimes, commodities are defined as raw commodities (such as cereals or roundwood) as opposed to products which would be the outcome of a production process (bread or wood panels). But it is not always the case and in the data sources the two words are used interchangeably. Eurostat Comext has a variable name "product_nc" that refers to traded commodities and products, while the similar variable is called "commodity_code" in UN Comtrade. Faostat refers to products as "item_code". The term "item" is also used in other contexts for land use datasets to refer to particular land use types.

The following Columns names are related to commodities and products names and codes in the different data sources, as described in the harmonization table 
[config_data/column_names.csv](../biotrade/config_data/column_names.csv). 

| biotrade                 | comext     | comtrade_human | comtrade_machine | faostat_production | faostat_trade |
|---------------------|------------|----------------|------------------|--------------------|---------------|
| product_code        | PRODUCT_NC | commodity_code | cmdcode          | item_code          | item_code     |
| product             |            |                |                  | item               | item          |
| product_description |            | commodity      | cmddesce         |                    |               |


<!-- #region tags=[] -->

## Countries and geographical areas

Bilateral trade describes the physical movement of products between an exporting country 
and an importing country. For example, when Brazil exports a commodity to the 
Netherlands. The flow is reported by Brazil as an export quantity to the Netherlands, 
and the same physical flow is reported by the Netherlands as an import quantity from 
Brazil. In theory the quantities would be equal, and the values should differ by 
shipping and insurance costs. In practice reported quantities may differ as explained in 
the section on mirror flows below. 

The following column names describe countries and geographical areas :


| biotrade           | comext    | comtrade_human | comtrade_machine | faostat_production | faostat_trade         | faostat_land | faostat_country |
|---------------|-----------|----------------|------------------|--------------------|-----------------------|--------------|-----------------|
| reporter_code | DECLARANT | reporter_code  | rtcode           | area_code          | reporter_country_code | area_code    |                 |
| reporter      |           | reporter       | rttitle          | area               | reporter_countries    | area         |                 |
| reporter_iso  |           | reporter_iso   | rt3iso           |                    |                       |              |                 |
| partner_code  | PARTNER   | partner_code   | ptcode           |                    | partner_country_code  |              |                 |
| partner       |           | partner        | pttitle          |                    | partner_countries     |              |                 |
| partner_iso   |           | partner_iso    | pt3iso           |                    |                       |              |                 |

Country codes are different in the 3 data sources, UN Comtrade, Eurostat Comext and FAOSTAT.
A mapping table between FAOSTAT country codes, ISO3 and UN codes is provided in [config_data/faostat_country_groups.csv](../biotrade/config_data/faostat_country_groups.csv).
<!-- #endregion -->

<!-- #region tags=[] -->
### Mirror flows

Quote from the [World Bank World Integrated Trade Solutions]()

> "In a perfect world, country A reported imports from country B would match with country B reported exports to country A. Consequently, this would make mirroring (using information from the partner when a country does not report its trade) a transparent and error-free process.
> However, this is not the case for the following reasons:

> - In UN COMTRADE, imports are recorded cif (cost insurance and freight) while exports are fob (free on board). This may represent a 10% to 20% difference.
> - Despite all efforts made by national and international agencies, data quality may vary among countries.
> - For a given country, imports are usually recorded with more accuracy than exports because imports generally generate tariff revenues while exports don't.
> - At a detailed level, a same good may be recorded in different categories by the exporter and the importer."
<!-- #endregion -->

## Flags and quantity estimations

The various sources use flags to notify about data modification steps.

### FAOSTAT flags

For example reusing data frames defined above, you can see flags used in the FAOSTAT crop production data:

    cp.flag.unique()
    # array(['F', 'Im', '', '*', 'Fc', 'M', 'A', 'R'], dtype=object)

FAOSTAT crop production flags are described in the
[crop production bulk download additional files](https://www.fao.org/faostat/en/#data/QCL)

| Flag    | Description                              |
|---------|------------------------------------------|
| *       | Unofficial figure                        |
| <blank> | Official data                            |
| A       | Aggregate                                |
| F       | FAO estimate                             |
| Fc      | Calculated data                          |
| Im      | FAO data based on imputation methodology |
| M       | Data not available                       |


### Flags used in the UN Comtrade data:



    ft_can.flag.unique()
    # array(['', 'R', 'Cv', '*', 'P', 'A'], dtype=object)

[Methodology Guide for UN Comtrade User on UN Comtrade Upgrade 2019](https://comtrade.un.org/data/MethodologyGuideforComtradePlus.pdf)

> "Estimation of quantity and net weight is performed in two cases: i) when data has not been provided; and
ii) provided data is disregarded as “it is considered an outlier”, or it does not conform with, and cannot
mathematically be converted into an WCO RU. The aim is to estimate all quantity information that is not
available i.e. to provide value added to users and to facilitate aggregation; however, only estimated quantity
that reaches certain level of reliability will be shown in UN Comtrade.
The quantity or net weight provided that are considered outliers are blanked out and their values are
estimated.

> a) Using (partially) reported quantity and/or net weight in the same commodity flow
(Weighted Unit Value):
In order to use the (partially) reported quantity and/or net weight information as a reliable basis for
estimating non-reported quantity and/or net weight within the same commodity flow (6-digit
imports and exports) of a country, the trade value share of the data with properly reported quantity
and/or net weight must pass a certain threshold, which is 20% for OECD and 50% for non-OECD
countries. Nevertheless, even if it does not pass the threshold, the available information will still
be used for estimation. In both cases a weighted unit value derived from the value/quantity or
value/weight ratio of the properly reported data is used to estimate the missing quantity and weight
information. However, the estimated quantity from a low trade value share is considered as not-
reliable, and the estimated quantity is suppressed at public dissemination.
Trade data is often reported at a more detailed level than the 6-digit level for which WCO RU are
defined and at which quantity and weight information is maintained in UN Comtrade. Quantities
are estimated at the most detailed level of reported data. There are numerous cases where countries
apply different quantity units within the same 6-digit commodity heading. In this case, the
estimation is attempted first at tariff line level using a weighted unit value, and then the units are
converted into standard units for aggregation.

> b) Using Standard Unit Value (SUV)
The so-called Standard Unit Value (SUV) is used to estimate net weight and quantity whenever
Weighted Unit Value cannot be used (i.e., when partially reported quantity and/or net weight data
do not meet the thresholds described above or when no such data is reported or available). SUVs
at sub-heading level are calculated at the end of the year for all commodity flows at tariff line level
using the available data of the latest reporting year. The Standard Unit Value of a specific
commodity flow is defined as the median unit value of all reporting countries from the prior year
after the elimination of outliers. SUVs are used for quantity and/or net weight estimation and
detection of outliers in the data of the subsequent reporting year."



## Trade value, quantity and weight

Trade value, quantity and weight have the following names in the Eurostat Comext and UN Comtrade datasets:

| biotrade          | comext        | comtrade_human      | comtrade_machine |
|--------------|---------------|---------------------|------------------|
| quantity     | SUP_QUANTITY  | qty                 | qtaltcode        |
| weight       | QUANTITY_TON  |                     |                  |
| net_weight   |               | netweight_kg        | netweight        |
| gross_weight |               | gross_weight_kg     | grossweight      |
| trade_value  | VALUE_1000ECU | trade_value_usd     | tradevalue       |
| cif_value    |               | cif_trade_value_usd | cifvalue         |
| fob_value    |               | fob_trade_value_usd | fobvalue         |




### FAOSTAT elements describe quantity and value

FAOSTAT data is in long format, and therefore the quantities are simply storred in the "value" column, further defined by the element

| biotrade          | faostat_production | faostat_trade | faostat_land |
|--------------|--------------------|---------------|--------------|
| element_code | element_code       | element_code  | element_code |
| element      | element            | element       | element      |

In long format, the "element" column describes the different variables used quantify trade flows in the faostat forestry trade and crop trade datasets.

    ft_can = faostat.db.select(table="forestry_trade", reporter=["Canada"])
    ft_can[["element", "unit"]].drop_duplicates()


| element         | unit     |
|-----------------|----------|
| export_value    | 1000 US\$ |
| import_value    | 1000 US\$ |
| export_quantity | m3       |
| import_quantity | m3       |
| export_quantity | tonnes   |
| import_quantity | tonnes   |

Long tables can be pivoted to a wide format with:

    index = ['reporter_code', 'reporter', 'partner_code', 'partner', 'product_code', 'product', 'period', 'year', 'unit', 'flag']
    ft_can.pivot(index=index, columns="element",values="value")

Which will create the columns: export_quantity  export_value  import_quantity  import_value

    cp1[["element", "unit"]].drop_duplicates()

The crop production dataset has more elements

| element                       | unit      |
|-------------------------------|-----------|
| area_harvested                | ha        |
| production                    | tonnes    |
| yield                         | hg/ha     |
| stocks                        | No        |
| stocks                        | Head      |
| stocks                        | 1000 Head |
| yield                         | 100mg/An  |
| laying                        | 1000 Head |
| production                    | 1000 No   |
| producing_animals_slaughtered | Head      |
| yield                         | hg/An     |
| yield                         | hg        |
| yield_carcass_weight          | hg/An     |
| yield_carcass_weight          | 0.1g/An   |
| producing_animals_slaughtered | 1000 Head |


## Units

Some datasets have a unit column which defines the unit used in the main variable of interest.

| biotrade       | comext | comtrade_human | comtrade_machine | faostat_production | faostat_trade | faostat_land | faostat_country |
|-----------|--------|----------------|------------------|--------------------|---------------|--------------|-----------------|
| unit_code |        | qty_unit_code  | qtcode           |                    |               |              |                 |
| unit      |        | qty_unit       | qtdesc           | unit               | unit          | unit         |                 |

<!-- #region -->
# Harmonization

## Automated renaming of column names

Variable name harmonization is performed after the data is downloaded 
based on a mapping between the column names in different sources in 
[config_data/column_names.csv](../biotrade/config_data/column_names.csv).


Columns are renamed in the [comtrade pump](../biotrade/comtrade/pump.py) as such:

    mapping = self.column_names.set_index("comtrade_machine").to_dict()["biotrade"]
    df.rename(columns=mapping, inplace=True)

Columns are renamed in the [faostat pump](../biotrade/faostat/pump.py) as such :

    mapping = self.column_names.set_index(column_renaming).to_dict()["biotrade"]
    df.rename(columns=mapping, inplace=True)


# Countries, regions and continents


## China, Macao, Hong Kong and Taiwan

The sources report different region groupings for China. FAOSTAT reports data for China 
mainland, Taiwan, Hong Kong and Maco individually as well as an aggregate of all 4 
called China (faostat code 351). UN Comtrade reports data for China (UN code 156 which 
is an aggregate of China and Taiwan), Hong Kong and Macao separately.

To provide comparable data, we can sum the FAOSTAT values from China, mainland (faostat 
code 41) with china, Taiwan (faostat code 214) into china + taiwan (faostat code 357) 
which corresponds to the UN Comtrade code 156 and we assign it the ISO code CHN. Hong 
Kong and Macao remain reported separately.



<!-- #endregion -->
