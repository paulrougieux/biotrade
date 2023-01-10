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
and forest products trade data from international data sources. The software provides 
methods to compare data across sources as well as methods to aggregate values and 
present the most important products traded and the most important trade partners.


# Statement of need

Similar download tools exist in the R and python packaging ecosystems. The R package 
`FAOSTAT` makes it possible to download FAOSTAT and World Bank data. The python package 
`eurostat` can download Eurostat data, so does the R package `eurostat` 
@lahti2017retrieval. 

The advantage of the biotrade package is that it harmonises variable names across data 
sources, so that R or python code depending on our naming convention becomes more 
readable when moving from one data source to another.

The `biotrade` package is useful as a preparatory step in several use cases in 
environmental economics and life cycle analysis: (1) to compute the ecological footprint 
of trade (2) to compute demand, production and prices in market models of the 
agriculture or forest sector.

Data conversion between UN Comtrade codes and FAOSTAT codes make it possible to extend 
FAOSTAT bilateral trade datasets with the latest updated data from Comtrade.


# Package structure and data update

The package is structured by data sources. Each source has a series of methods to 
download and aggregate data. Some functions that perform comparison and data aggregation 
are common to all data sources.
The data is cached in a local database which can be either an SQLite database (the 
default) or a PostGreSQL database (user defined through an environment variable).

The following code updates the FAOSTAT data on Land use



# Aggregation and comparison functions


Example use of the `nlargest` function to compute the 3 largest agricultural area of 
wheat 


Example use of the `nlargest` function to compute the 5 largest import flows of sawnwood 
by trade value and weight from the UN Comtrade dataset.

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



