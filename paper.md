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


# Summary

The package is structured by data source, each data source has a series of method to 
download and aggregate data. Some functions that perform comparison and data aggregation 
are common to all data sources.


# Data update

One of the main aim of the package is to enable regular update of agriculture and forest 
products trade related data from international data sources.


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


TODO: structure paper as necessary for the Journal of open source software.
https://joss.readthedocs.io/en/latest/submitting.html#example-paper-and-bibliography
