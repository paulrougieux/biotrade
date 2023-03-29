---
title: 'Biotrade: A Python package to analyse the international trade of bio-based products'
tags:
  - python
  - life cycle analysis
  - forest
  - agriculture
  - land footprint
authors:
  - name: Paul Rougieux
    orcid: 0000-0001-9073-9826
    affiliation: "1"
    corresponding: true
  - name: Selene Patani
    orcid: 0000-0001-8601-3336
    affiliation: "2"
  - name: Mirco Migliavacca
    orcid: 0000-0003-3546-8407
    affiliation: "1"
affiliations:
 - name: European Commission Joint Research Centre, Ispra, Italy
   index: 1
 - name: JRC Consultant, ARCADIA SIT s.r.l., Vigevano (PV), Italy
   index: 2
date:
bibliography: paper.bib
---

<!--
The following comments will not appear in the paper.

- Journal of Open Source Software (JOSS)- Paper submission guidelines
  https://joss.readthedocs.io/en/latest/submitting.html

- Compile this paper to a pdf document with the script specified in .gitlab-ci.yml. JOSS
  uses the openjournals/inara docker image and compiles the document with the following
  script:

      inara -p -o pdf paper/paper.md

End comments.
-->

# Summary

The aim of the `biotrade` package is to enable regular updates of agriculture, fisheries
and forest products trade data from the following international data sources: FAOSTAT
[-@faostat2023], UN Comtrade [-@comtrade2023] and the World Bank [-@worldbank2023]. The
software provides methods to join data across sources as well as to aggregate and rank
the most important products and the most important countries. It also provides tools to
assess the quality of the data, by comparing for example mirror flows or unit prices of
trade.


# Statement of need

Disciplines such as environmental economics or Life Cycle Analysis are concerned with
research questions such as: (1) what is the ecological footprint of trade? or (2) how do
agricultural market prices react to natural disasters? To analyse these questions on a
global scale, researchers use models based on time series of commodities production and
trade with data covering many countries and many years in a panel format. Every month,
or a few times per year, countries update their data in a centralised way through
international organisations such as the UN Food and Agriculture Organisation (FAO) or
the UN Comtrade. This data is made available in the form of files on a public website.
Dedicated tools can help download data into statistical programming software such as
Julia, Matlab, Python, R or SAS. These tools generally also have methods to perform
consistency checks or data aggregation. They make it possible to re-run an analysis,
generate plots and model outputs when new data become available in a time series. Thus,
they facilitate reproducible research.

Such download tools exist already in the R and python packaging ecosystems. The R
package `FAOSTAT` [@kao2022faostat] can download and prepare FAOSTAT data. The R package
WDI [@arelbundock2022] downloads data from the World Bank including the World
Development Indicators. The python package `eurostat` [@cazzaniga2022eurostat] can
download Eurostat data, so does the R package `eurostat` [@lahti2017retrieval].

There is however no package that can download from all these sources under a common naming
scheme. Also these packages only load data frames and leave it up to the user to save
their data for later. An advantage of the `biotrade` package is that it creates a local
copy inside a database.
The `biotrade` package also harmonises variable names across data sources. The benefit
is that python or R code become more readable when moving from one data source to
another. The table \autoref{tab:colnames} shows the naming convention and some of the
variable names used across different sources.

\label{tab:colnames}

| biotrade       | comtrade         | faostat production   | faostat trade        | world_bank     |
| -------------- | ---------------- | -------------------- | -------------------- | -------------- |
| year           | year             | year                 | year                 | year           |
| reporter       | reporter         | area                 | reporter_countries   | country_name   |
| partner        | partner          |                      | partner_countries    |                |
| product_code   | commodity_code   | item_code            | item_code            |                |


One issue when working with UN Comtrade and FAOSTAT data, is that country codes are
different between the two sources. The package contains a mapping table between country
codes in the 2 coding system, as well as ISO codes and continent and sub-continent
aggregates. In addition the UN Comtrade product classification is vastly larger than the
FAOSTAT product classification. The `biotrade` package also contains a mapping table
between FAOSTAT product codes and the Harmonized System classification used in Comtrade.
The country and product mapping tables make it possible to convert UN comtrade data into
the FAOSTAT format, so that it can be analysed in the same way. Aggregation functions
are also based on these country and product mapping tables.


# Data update and package structure

Thanks to the SQL Alchemy [@bayer2012] abstraction layer `biotrade` can use different
database engines. The data is cached in a local database which can be either an SQLite
database (the default) or a PostGreSQL database (user defined through an environment
variable). Database tables are structured with unique constraints to ensure that there
is no duplication when data is updated.


The package is structured by data sources. Each source has a series of methods to
download and aggregate data. Some functions that perform comparison and data aggregation
are common to all data sources.

The following python code updates crop production data by downloading it from the
FAOSTAT API:

```
from biotrade.faostat import faostat
faostat.pump.update(["crop_production"])
```

The following code loads sawnwood other (HS code 440799) and soya beans (HS code 120190)
trade reported by Italy (reporter code 381) in 2020 from the Comtrade API:

```
from biotrade.comtrade import comtrade
sawnwood99_it = comtrade.pump.download_df(cc = "440799", r="381", ps="2020")
soya_beans_it = comtrade.pump.download_df(cc = "120190", r="381", ps="2020")
```


# Aggregation and comparison

Example use of the `nlargest` function to display the 3 largest wheat producers
globally:

```
from biotrade.faostat import faostat
from biotrade.common.aggregate import nlargest
wheat = faostat.db.select("crop_production", product="wheat")
wheat_largest =  nlargest(
    # Remove aggregates (continents) and keep only recent years
    wheat.query("reporter_code < 1000 and year > year.max() - 10"),
    value_vars="value",
    agg_groups=["reporter", "product"],
    slice_groups=["element", "unit"],
    n=5
)
wheat_largest

#    product         element                   reporter    unit        value
# 0    wheat  area_harvested                      India      ha   30205021.0
# 1    wheat  area_harvested         Russian Federation      ha   25698803.1
# 2    wheat  area_harvested                      China      ha   24189501.9
# 3    wheat  area_harvested            China, mainland      ha   24187166.0
# 4    wheat  area_harvested   United States of America      ha   17344652.9
# 5    wheat      production                      China  tonnes  128605992.0
# 6    wheat      production            China, mainland  tonnes  128600610.0
# 7    wheat      production                      India  tonnes   95949997.0
# 8    wheat      production         Russian Federation  tonnes   65938100.0
# 9    wheat      production   United States of America  tonnes   54897588.9
# 10   wheat           yield                    Ireland   hg/ha      91991.6
# 11   wheat           yield                New Zealand   hg/ha      89421.0
# 12   wheat           yield                    Belgium   hg/ha      87399.3
# 13   wheat           yield                Netherlands   hg/ha      87030.2
# 14   wheat           yield United Kingdom of Great...   hg/ha      79164.2
```

The `biotrade` package can also be used from the R statistical software, thanks to an
interface with python called reticulate [@ushey2023]. Example loading FAOSTAT crop production data
from within R and ranking countries with the `nlargest` function. Rows are aggregated by
`agg_groups`, sorted by the first of the `value_vars` and slicing takes the first 5 rows
in each slice group:

```
# This is the only R code example in this document
library(reticulate)
library(dplyr)
py_run_string("from biotrade.faostat import faostat")
py_run_string("from biotrade.common.aggregate import nlargest")
wheat <- py$faostat$db$select("crop_production", product="wheat")
wheat_largest <- wheat %>%
    # Remove aggregates (continents) and keep only recent years
    filter(reporter_code < 1000 & year > max(year) - 10) %>%
    py$nlargest(value_vars="value",
                agg_groups=c("reporter", "product"),
                slice_groups=c("element", "unit"),
                n=5)
str(wheat_largest)
# 'data.frame':   15 obs. of  5 variables:
#  $ product : chr  "wheat" "wheat" "wheat" "wheat" ...
#  $ unit    : chr  "ha" "ha" "ha" "ha" ...
#  $ element : chr  "area_harvested" "area_harvested" "area_harvested" "area_harvested" ...
#  $ reporter: chr  "India" "Russian Federation" "China" "China, mainland" ...
#  $ value   : num  30205021 25698803 24189502 24187166 17344653 ...
#  - attr(*, "pandas.index")=RangeIndex(start=0, stop=15, step=1)
```


- TODO: Illustrate the unit price of trade based on Comtrade data
- TODO: Illustrate FAOSTAT Bilateral trade Compared to Comtrade bilateral trade
- TODO: Illustrate other aggregation and comparison functions


# Conclusion

The `biotrade` package is a download and preparation tool for agriculture and forestry
production and trade data. It enforces a naming convention on variables to make sure
comparable variable names are used across data sources. As source APIs tend to change
their access mechanisms every few years; the goal of the package maintainers is to
abstract away those changes and provide a unified method to update data, that should
continue to work with future updates. The package provides convenient methods to store
intermediate results and to select data. We encourage users to install the `biotrade`
package, to test it and to send feedback through the [issues
page](https://gitlab.com/bioeconomy/forobs/biotrade/-/issues).


# References


