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

<!-- Paper submission guidelines
https://joss.readthedocs.io/en/latest/submitting.html
-->

# Summary

The aim of the `biotrade` package is to enable regular updates of agriculture, fisheries 
and forest products trade data from the following international data sources: FAOSTAT 
[-@faostat2023], UN Comtrade [-@comtrade2023] and the World Bank [-@worldbank2023]. 
Additional data sources might be added in the future. The software provides methods to 
compare data across sources as well as methods to aggregate and rank the most important 
products and the most important countries. It also provides tools to assess the quality 
of the data, by comparing for example mirror flows or unit prices of trade.


# Statement of need

In disciplines such as environmental economics and Life Cycle Analysis (LCA), 
researchers are concerned with the following research questions for example: (1) what is 
the ecological footprint of trade? or (2) how do trade prices react to natural 
disasters? The models used to answer these questions are based on time series of 
bio-based commodities with data covering many countries and many years in a panel 
format. Every month, or a few times per year, countries update their data through a 
mechanism centralised by international organisations such as the UN Food and Agriculture 
Organisation or the UN Comtrade. This data is made available in the form of files on a 
public server. There are dedicated tools that facilitate data download into statistical 
programming software. These tools generally also have methods to perform consistency 
checks or data aggregation. They make it possible to re-run an analysis (plots and 
models) when new data becomes available. Thus, they facilitate reproducible research. 

Such download tools exist already in the R and python packaging ecosystems. The R 
package `FAOSTAT` [@kao2022faostat] can download and prepare FAOSTAT data. The R package 
WDI can download World Bank data. The python package `eurostat` [@cazzaniga2022eurostat] 
can download Eurostat data, so does the R package `eurostat` [@lahti2017retrieval]. 
There is however no package that can download all these sources under a common naming 
scheme. Also these packages only load data frames and leave it up to the user to save 
their data.

The advantage of the `biotrade` package is that it creates a local copy of the source 
database on disk. Thanks to the SQL Alchemy abstraction layer `biotrade` can store the 
data in many different database engines. Currently SQLite is implemented by default, and 
PostGreSQL can be used when available. The `biotrade` package also harmonises variable 
names across data sources. The benefit is that R or python code becomes more readable 
when moving from one data source to another. Table \autoref{tab:colnames} shows the 
naming convention and some of the variable names used in the different sources.


\label{tab:colnames}

| biotrade       | comtrade         | faostat_production   | faostat_trade        | world_bank     |
| -------------- | ---------------- | -------------------- | -------------------- | -------------- |
| year           | year             | year                 | year                 | year           |
| reporter       | reporter         | area                 | reporter_countries   | country_name   |
| partner        | partner          |                      | partner_countries    |                |
| product_code   | commodity_code   | item_code            | item_code            |                |


One issue when working with UN Comtrade and FAOSTAT, is that country codes are different 
in the two data sources. The `biotarde` package contains mapping tables between 
countries in the 2 coding system, as well as ISO codes and continent and sub-continent 
aggregates. In addition the UN COmtrade product classification is vastly larger than the 
FAOSTAT product classification. The `biotrade` package also contains a mapping table 
between FAOSTAT product codes and the Harmonized System classification. 

Data conversion between UN Comtrade codes and FAOSTAT codes make it possible to extend 
FAOSTAT bilateral trade datasets with the latest updated data from Comtrade.


# Data update and Package structure

The package is structured by data sources. Each source has a series of methods to 
download and aggregate data. Some functions that perform comparison and data aggregation 
are common to all data sources.
The data is cached in a local database which can be either an SQLite database (the 
default) or a PostGreSQL database (user defined through an environment variable).

The following code updates the FAOSTAT data on Land use



# Aggregation and comparison functions


Example use of the `nlargest` function to compute the 3 largest agricultural area of 
wheat globally.


```
swdlargep <- swd %>%
    py$nlargest(value_vars="value",
                agg_groups=c("reporter", "partner", "element"),
                slice_groups=c("flow", "element"),
                n=5) %>%
    arrange(desc(sum(value))) %>%
    mutate(value_million = round(value /1e6))
```



TODO: Illustrade the unit price of trade based on Comtrade data

TODO: Illustrade FAOSTAT Bilateral trade Compared to Comtrade bilateral trade

TODO: Illustrade other aggregation and comparison functions


# Conclusion 


The role of a download tools is to abstract away changes in the source API, to enforce a 
naming convention and to store intermediate results while providing convenient methods 
to select the data. We encourage users to try out the `biotrade` package from python and 
even from R thanks to the reticulate package. Please provide feedback on the issues 
page.




# References

