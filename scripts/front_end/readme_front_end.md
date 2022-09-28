# METADATA
Table names and metadata of the web platform output scripts. Currently 27 csv files are available.

> N.B.
>- Zero and NaN values are not reported.
>- Old country values are not reported.
>- Production, harvest area and trade data related to China are for:
>    - China mainland + Taiwan which corresponds to UN Comtrade code 156
>    - Removed FAOSTAT data on China Mainland because it doesn't have a UN country code
>    - Removed FAOSTAT data on Taiwan (not reported separately by UN Comtrade) to avoid double counting Taiwan already included in UN code 156
>    - Hong Kong and Macao are reported separately both in the UN Comtrade and FAOSTAT data.

## reporter_list.csv
Csv that contains the list of all reporters. Can be used also for partners. There are 3 columns:
- `reporter_code`: code of data reporter
- `reporter_name`: name of the reporter
- `status`: can assume three values
    - "iso3" which means that the reporter is identified through the iso3 code;
    - "old" meaning that the reporter does not exist anymore but it was in the past and older data can be associated to it;
    - "agg" which represents an aggregation of reporters

## product_list.csv
Csv that contains the list of all products. There are 3 columns:
- `product_code`: FAO code of the product
- `product_name`: name of the product
- `key_product_flag`: 1 if the product is a key primary product; 0 otherwise

## key_product_tree_list.csv
Csv to create the product tree according to FAO technical report 2011.
Key products selected in this csv are coffee, cocoa, soybeans, palm oil fruit and cattle.
There are 6 columns:
- `primary_product_code`: code of the primary product name (just for sub-setting purposes)
- `parent_product_name`: parent product name, left side of each branch of the trees
- `parent_product_code`: FAO code of the parent product
- `child_product_name`: name of the product derived from the parent, right side of the tree
- `child_product_code`: FAO code of the child
- `bp_flag`: 1 indicates co-products or byproducts (e.g., cake and oil); 0 otherwise

## harvested_area_annual_variation.csv
Csv that contains annual data of harvested area for each of the key products and the related reporters. There are 5 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## production_annual_variation.csv
Csv that contains annual data of production for each of the key products and the related reporters. There are 5 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## harvested_area_average_legend.csv
Csv that contains for each product (not only key products) the minimum and maximum values associated to the interval ranking of the average harvested area across periods and countries, together with the specific units. Each interval is defined with the left boundary (minimum) excluded, while the right extreme (maximum) is included. There are 5 columns:
- `interval`: value between 0 and 10 which allows to rank the harvested area across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual harvested area reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%
- `min_value`: minimum value of the average of the total annual harvested area for the associated interval and product code (value not included)
- `max_value`: maximum value of the average of the total annual harvested area for the associated interval and product code (value included)
- `product_code`: FAO code of the product
- `unit`: measurement units of the data

## harvested_area_average.csv
Csv that contains for each product (not only key products) the average value of the harvested area across reporters and for each reporter the average value of the harvested area across products. 
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of harvested area associated to the product (or 
reporter) by the number of years of the time period.
Percentage values are also reported for products and reporters. The average value of the total annual harvested area for the specific reporter and product together the associated interval ranking are specified. There are 10 columns:
- `reporter_code`: code of data reporter
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `product_code`: FAO code of the product
- `reporter_code_avg_value`: average value of the total annual harvested area related to the specific reporter code, across all the associated products
- `product_code_percentage`: percentage value of harvested area related to the specific product code, across all products of the reporter code 
- `product_code_avg_value`: average value of the total annual harvested area related to the specific product code, across all the associated reporters
- `reporter_code_percentage`: percentage value of harvested area related to the specific reporter code, across all reporters of the product code
- `avg_value`: average value of the total annual harvested area related to the specific reporter and product code
- `interval`: value between 0 and 10 which allows to rank the harvested area across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual harvested area reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%

## production_average_legend.csv
Csv that contains for each product (not only key products) the minimum and maximum values associated to the interval ranking of the average production across periods and countries, together with the specific units. Each interval is defined with the left boundary (minimum) excluded, while the right extreme (maximum) is included. There are 5 columns:
- `interval`: value between 0 and 10 which allows to rank the production across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual production reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%
- `min_value`: minimum value of the average of the total annual production for the associated interval and product code (value not included)
- `max_value`: maximum value of the average of the total annual production for the associated interval and product code (value included)
- `product_code`: FAO code of the product
- `unit`: measurement units of the data

## production_average.csv
Csv that contains for each product (not only key products) the average value of the production across reporters and for each reporter the average value of the production across products. 
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of production associated to the product (or 
reporter) by the number of years of the time period.
Percentage values are also reported for products and reporters. The average value of the total annual production for the specific reporter and product together the associated interval ranking are specified. There are 10 columns:
- `reporter_code`: code of data reporter
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `product_code`: FAO code of the product
- `reporter_code_avg_value`: average value of the total annual production related to the specific reporter code, across all the associated products
- `product_code_percentage`: percentage value of production related to the specific product code, across all products of the reporter code
- `product_code_avg_value`: average value of the total annual production related to the specific product code, across all the associated reporters
- `reporter_code_percentage`: percentage value of production related to the specific reporter code, across all reporters of the product code
- `avg_value`: average value of the total annual production related to the specific reporter and product code
- `interval`: value between 0 and 10 which allows to rank the production across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual production reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%

## harvested_area_trends.csv
Csv that contains trends of harvested area associated to the key products. For each reporter and each key product, the time series after 1985 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of harvested area associated to the most recent year with respect to the average value of the 5 previous years and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: most recent year of analyzed data
- `period_change`: aggregation period considered to obtain the relative and absolute changes of harvested area yyyy-yyyy
- `period_regression`: period of the most recent time series segment, obtained from the segmented regression analysis. The Mann-Kendall test is performed over the data included in this period yyyy-yyyy
- `relative_change`: harvested area (associated to the specific product and reporter) of the most recent year minus the average value over the period change, divided by this last value
- `absolute_change`: harvested area (associated to the specific product and reporter) of the most recent year minus the average value over the period change
- `mk_slope`: annual variation (slope) obtained from the Mann-Kendall test
- `mk_significance_flag`: 1 means that the statistical test is valid (significance level = 0.05), 0 otherwise
- `unit`: measurement units of the data

## production_trends.csv
Csv that contains trends of production associated to the key products. For each reporter and each key product, the time series after 1985 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of production associated to the most recent year with respect to the average value of the 5 previous years and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: most recent year of analyzed data
- `period_change`: aggregation period considered to obtain the relative and absolute changes of production yyyy-yyyy
- `period_regression`: period of the most recent time series segment, obtained from the segmented regression analysis. The Mann-Kendall test is performed over the data included in this period yyyy-yyyy
- `relative_change`: production (associated to the specific product and reporter) of the most recent year minus the average value over the period change, divided by this last value
- `absolute_change`: production (associated to the specific product and reporter) of the most recent year minus the average value over the period change
- `mk_slope`: annual variation (slope) obtained from the Mann-Kendall test
- `mk_significance_flag`: 1 means that the statistical test is valid (significance level = 0.05), 0 otherwise
- `unit`: measurement units of the data

## faostat_annual_variation.csv
Csv that contains annual data of Faostat source related to bilateral trade quantities for each of the key products as well as the related reporters and partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_annual_variation_mf.csv
Csv that contains annual data of Faostat source related to bilateral trade quantities for each of the key products as well as the related reporters and partners. Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation.csv
Csv that contains annual data of Faostat source related to bilateral trade monetary values for each of the key products as well as the related reporters and partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation_mf.csv
Csv that contains annual data of Faostat source related to bilateral trade monetary values for each of the key products as well as the related reporters and partners. Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_annual_variation.csv
Csv that contains annual data of Comtrade source related to bilateral trade quantities for each of the key products as well as the related reporters and partners. Data are associated to import flows.
As general rule, data taken by the Comtrade source have year frequency. If there are more updated data with monthly frequency (generally the most recent year) and a lack of yearly data, an aggregation over the last 12 months is performed instead. There are 7 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data
- `estimate_flag`: 1 when the quantity is the aggregation of the last 12 monthly data, 0 means yearly data not manipulated

## comtrade_annual_variation_mf.csv
Csv that contains annual data of Comtrade source related to bilateral trade quantities for each of the key products as well as the related reporters and partners. Data are associated to mirror flows (exports).
As general rule, data taken by the Comtrade source have year frequency. If there are more updated data with monthly frequency (generally the most recent year) and a lack of yearly data, an aggregation over the last 12 months is performed instead. There are 7 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data
- `estimate_flag`: 1 when the quantity is the aggregation of the last 12 monthly data, 0 means yearly data not manipulated

## comtrade_value_annual_variation.csv
Csv that contains annual data of Comtrade source related to bilateral trade monetary values for each of the key products as well as the related reporters and partners. Data are associated to import flows.
As general rule, data taken by the Comtrade source have year frequency. If there are more updated data with monthly frequency (generally the most recent year) and a lack of yearly data, an aggregation over the last 12 months is performed instead. There are 7 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data
- `estimate_flag`: 1 when the monetary value is the aggregation of the last 12 monthly data, 0 means yearly data not manipulated

## comtrade_value_annual_variation_mf.csv
Csv that contains annual data of Comtrade source related to bilateral trade monetary values for each of the key products as well as the related reporters and partners. Data are associated to mirror flows (exports).
As general rule, data taken by the Comtrade source have year frequency. If there are more updated data with monthly frequency (generally the most recent year) and a lack of yearly data, an aggregation over the last 12 months is performed instead. There are 7 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data
- `estimate_flag`: 1 when the monetary value is the aggregation of the last 12 monthly data, 0 means yearly data not manipulated

## faostat_average.csv
Csv that contains for each key product the average value of trade quantity across importers, related to Faostat yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters. The percentage of a reporter has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 6 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `product_code_avg_value`: average value of the total annual trade quantity related to the specific product code, across all the associated reporters
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, across all reporters of the specific product code

## faostat_average_mf.csv
Csv that contains for each key product the average value of trade quantity across importers, related to Faostat yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners. The percentage of a partner has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 6 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `product_code_avg_value`: average value of the total annual trade quantity related to the specific product code, across all the associated partners
- `partner_code`: code of data partner
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, across all partners of the specific product code

## comtrade_average.csv
Csv that contains for each key product the average value of trade quantity across importers, related to Comtrade yearly data. Estimated values (aggregation of monthly data) are not used. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters. The percentage of a reporter has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 6 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `product_code_avg_value`: average value of the total annual trade quantity related to the specific product code, across all the associated reporters
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, across all reporters of the specific product code

## comtrade_average_mf.csv
Csv that contains for each key product the average value of trade quantity across importers, related to comtrade yearly data. Estimated values (aggregation of monthly data) are not used. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners. The percentage of a partner has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 6 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `product_code_avg_value`: average value of the total annual trade quantity related to the specific product code, across all the associated partners
- `partner_code`: code of data partner
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, across all partners of the specific product code

## faostat_average_eu_row.csv
Csv that contains for each key product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Faostat yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with EU27 and ROW. The percentage of a partner has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product and aggregated reporter. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 7 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code

## faostat_average_eu_row_mf.csv
Csv that contains for each key product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Faostat yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with EU27 and ROW. The percentage of a reporter has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product and aggregated partner. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 7 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code

## comtrade_average_eu_row.csv
Csv that contains for each key product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Comtrade yearly data. Estimated values (aggregation of monthly data) are not used. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with EU27 and ROW. The percentage of a partner has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product and aggregated reporter. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 7 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code

## comtrade_average_eu_row_mf.csv
Csv that contains for each key product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Comtrade yearly data. Estimated values (aggregation of monthly data) are not used. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with EU27 and ROW. The percentage of a reporter has to be comprised in the 95% of the descendent cumulative sum of trade quantity for a specific product and aggregated partner. The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 7 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code