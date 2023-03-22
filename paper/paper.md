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

- Paper submission guidelines https://joss.readthedocs.io/en/latest/submitting.html

- Compile this paper to a pdf document with the script specified in .gitlab-ci.yml. The
  Journal of Open Source Software uses the openjournals/inara docker image and compiles
  the document with the following script:

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

Disciplines such as environmental economics or Life Cycle Analysis (LCA)
are concerned with research questions such as: (1) what is the ecological footprint of
trade? or (2) how do agricultural market prices react to natural disasters? To analyse
these questions on a global scale, researchers use use models based on time series of
commodities production and trade with data covering many countries and many years in a
panel format. Every month, or a few times per year, countries update their data in a
centralised way through international organisations such as the UN Food and Agriculture
Organisation or the UN Comtrade. This data is made available in the form of files on a
public website. Dedicated tools can help download data into statistical programming
software such as Matlab, python, R or SAS. These tools generally also have methods to
perform consistency checks or data aggregation. They make it possible to re-run an
analysis, generate plots and model outputs when new data become available in a time
series. Thus, they facilitate reproducible research.

Such download tools exist already in the R and python packaging ecosystems. The R
package `FAOSTAT` [@kao2022faostat] can download and prepare FAOSTAT data. The R package
WDI [@arelbundock2022] downloads data from the World Bank including the World
Development Indicators. The python package `eurostat` [@cazzaniga2022eurostat] can
download Eurostat data, so does the R package `eurostat` [@lahti2017retrieval].

There is however no package that can download all these sources under a common naming
scheme. Also these packages only load data frames and leave it up to the user to save
their data for later. An advantage of the `biotrade` package is that it creates a local
copy inside a database. The database is structured with unique constraints to ensure
that there is no duplication when the data is updated.

The `biotrade` package also harmonises variable names across data sources. The benefit
is that R or python code becomes more readable when moving from one data source to
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


# Data update and Package structure

Thanks to the SQL Alchemy [@sqlalchey] abstraction layer `biotrade` can use different
database engines. The data is cached in a local database which can be either an SQLite
database (the default) or a PostGreSQL database (user defined through an environment
variable).

The package is structured by data sources. Each source has a series of methods to
download and aggregate data. Some functions that perform comparison and data aggregation
are common to all data sources.

The following code updates the FAOSTAT data on crop production

```{python}

```

The `biotrade` package can also be used from R, thanks to the reticulate package.
Example loading FAOSTAT crop production data from within R and ranking countries with
the `nlargest` function. Rows are aggregated by `agg_groups`, sorted by the first of the
`value_vars` and slicing takes the first 5 rows in each slice group:

```{r}
library(reticulate)
library(dplyr)
py_run_string("from biotrade.faostat import faostat")
py_run_string("from biotrade.common.aggregate import nlargest")
wheat = py$faostat$db$select("crop_production", product="wheat")
wheat_large_prod <- wheat %>%
    # Remove aggregates (continents) and keep only recent years
    filter(reporter_code < 1000 & year > max(wheat$year) - 10) %>%
    py$nlargest(value_vars="value",
                agg_groups=c("reporter", "product"),
                slice_groups=c("element"),
                n=5) %>%
    arrange(desc(sum(value))) %>%
    mutate(value_million = round(value /1e6))
```


# Aggregation and comparison

Example use of the `nlargest` function to display the 3 largest wheat producers
globally:

```

```


```{python}

# Place the source code for the figures inside a notebook in biotrade/notebooks
# refer to this notebook here
```

TODO: Illustrade the unit price of trade based on Comtrade data


TODO: Illustrade FAOSTAT Bilateral trade Compared to Comtrade bilateral trade

TODO: Illustrade other aggregation and comparison functions


# Conclusion

The role of a download tools is to abstract away changes in the source API, to enforce a
naming convention and to store intermediate results while providing convenient methods
to select the data. We encourage users to install the `biotrade` package, to test it and
to send feedback through the issues page.


# References


