# METADATA
Table names and metadata of the web platform output scripts. Currently 35 csv files are available.

> N.B.
>- Zero and NaN values are not reported.
>- Old country values are not reported.
>- France autonomous regions are not reported.
>- Production, harvest area and trade data related to China are for:
>    - China mainland + Taiwan which corresponds to UN Comtrade code 156
>    - Removed FAOSTAT data on China Mainland because it doesn't have a UN country code
>    - Removed FAOSTAT data on Taiwan (not reported separately by UN Comtrade) to avoid double counting Taiwan already included in UN code 156
>    - Hong Kong and Macao are reported separately both in the UN Comtrade and FAOSTAT data.

## product_list.csv
Csv that contains the list of faostat selected products. There are 3 columns:
- `product_code`: FAO code of the product
- `product_name`: name of the product
- `key_product_flag`: 1 if the product is a key primary product; 0 otherwise

## comtrade_product_list.csv
Csv that contains the list of comtrade selected products. There are 3 columns:
- `product_code`: FAO code of the product
- `product_name`: name of the product
- `commodity_name`: name of the commodity to which the product belongs

## key_product_tree_list.csv
Csv to create the product groups according to FAO technical report 2011.
Key products selected in this csv are coffee, cocoa, soybeans, palm oil fruit, maize, cattle, rubber, industrial and fuel wood.
There are 4 columns:
- `primary_product_code`: code of the primary product name (just for sub-setting purposes)
- `parent_product_code`: FAO code of the parent product, left side of each branch of the trees
- `child_product_code`: FAO code of the child product derived from the parent, right side of the tree
- `bp_flag`: 1 indicates co-products or byproducts (e.g., cake and oil); 0 otherwise

## harvested_area_annual_variation.csv
Csv that contains annual data of harvested area for each product and the related reporters. There are 5 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## production_annual_variation.csv
Csv that contains annual data of production for each product and the related reporters. There are 5 columns:
- `reporter_code`: code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## harvested_area_average_legend.csv
Csv that contains for each product the minimum and maximum values associated to the interval ranking of the average harvested area across periods and countries, together with the specific units. Each interval is defined with the left boundary (minimum) excluded, while the right extreme (maximum) is included. There are 5 columns:
- `interval`: value between 0 and 10 which allows to rank the harvested area across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual harvested area reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%
- `min_value`: minimum value of the average of the total annual harvested area for the associated interval and product code (value not included)
- `max_value`: maximum value of the average of the total annual harvested area for the associated interval and product code (value included)
- `product_code`: FAO code of the product
- `unit`: measurement units of the data

## harvested_area_average.csv
Csv that contains for each product the average value of the harvested area across reporters and for each reporter the average value of the harvested area across products. 
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
Csv that contains for each product the minimum and maximum values associated to the interval ranking of the average production across periods and countries, together with the specific units. Each interval is defined with the left boundary (minimum) excluded, while the right extreme (maximum) is included. There are 5 columns:
- `interval`: value between 0 and 10 which allows to rank the production across periods and countries for a specific product. 0 includes values lower than the 5% of the maximum average of the total annual production reported, while 10 covers values higher than the 95%. The other thresholds, for intervals between 1 and 9, are: 15%, 25%, 35%, 45%, 55%, 65%, 75%, 85%
- `min_value`: minimum value of the average of the total annual production for the associated interval and product code (value not included)
- `max_value`: maximum value of the average of the total annual production for the associated interval and product code (value included)
- `product_code`: FAO code of the product
- `unit`: measurement units of the data

## production_average.csv
Csv that contains for each product the average value of the production across reporters and for each reporter the average value of the production across products. 
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
Csv that contains trends of harvested area associated to each product. For each reporter and each product, the time series after 1985 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of harvested area associated to the most recent year with respect to the average value of the 5 previous years and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
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
Csv that contains trends of production associated to each products. For each reporter and each product, the time series after 1985 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of production associated to the most recent year with respect to the average value of the 5 previous years and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
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
Csv that contains annual data of Faostat source related to bilateral trade quantities of each product between reporters and partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_annual_variation_mf.csv
Csv that contains annual data of Faostat source related to bilateral trade quantities of each product between reporters and partners. Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation.csv
Csv that contains annual data of Faostat source related to bilateral trade monetary values of each product between reporters and partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation_mf.csv
Csv that contains annual data of Faostat source related to bilateral trade monetary values of each product between reporters and partners. Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_annual_variation_eu_row.csv
Csv that contains aggregated annual data of Faostat source related to bilateral trade quantities of each product between EU27 or ROW (reporters) and country partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_annual_variation_eu_row_mf.csv
Csv that contains aggregated annual data of Faostat source related to bilateral trade quantities of each product between country reporters and EU27 or ROW (partners). Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation_eu_row.csv
Csv that contains aggregated annual data of Faostat source related to bilateral trade monetary values of each product between EU27 or ROW (reporters) and country partners. Data are associated to import flows.
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_value_annual_variation_eu_row_mf.csv
Csv that contains aggregated annual data of Faostat source related to bilateral trade monetary values of each product between country reporters and EU27 or ROW (partners). Data are associated to mirror flows (exports).
Data taken by Faostat source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_annual_variation.csv
Csv that contains annual data of Comtrade source related to bilateral trade quantities of each product between reporters and partners. Data are associated to import flows.
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_annual_variation_mf.csv
Csv that contains annual data of Comtrade source related to bilateral trade quantities of each product between reporters and partners. Data are associated to mirror flows (exports).
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_value_annual_variation.csv
Csv that contains annual data of Comtrade source related to bilateral trade monetary values of each product between reporters and partners. Data are associated to import flows.
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_value_annual_variation_mf.csv
Csv that contains annual data of Comtrade source related to bilateral trade monetary values of each product between reporters and partners. Data are associated to mirror flows (exports).
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_annual_variation_eu_row.csv
Csv that contains aggregated annual data of Comtrade source related to bilateral trade quantities of each product between EU27 or ROW (reporters) and country partners. Data are associated to import flows.
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_annual_variation_eu_row_mf.csv
Csv that contains aggregated annual data of Comtrade source related to bilateral trade quantities of each product between country reporters and EU27 or ROW (partners). Data are associated to mirror flows (exports).
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_value_annual_variation_eu_row.csv
Csv that contains aggregated annual data of Comtrade source related to bilateral trade monetary values of each product between EU27 or ROW (reporters) and country partners. Data are associated to import flows.
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## comtrade_value_annual_variation_eu_row_mf.csv
Csv that contains aggregated annual data of Comtrade source related to bilateral trade monetary values of each product between country reporters and EU27 or ROW (partners). Data are associated to mirror flows (exports).
Data taken by Comtrade source have year frequency. There are 6 columns:
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code`: Comtrade code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## faostat_average.csv
Csv that contains for each product the average value of trade quantity imported by each country, related to Faostat yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with each reporter. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## faostat_average_mf.csv
Csv that contains for each product the average value of trade quantity imported by each country, related to Faostat yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with each partner. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## comtrade_average.csv
Csv that contains for each product the average value of trade quantity imported by each country, related to Comtrade yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with each reporter. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: Comtrade code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## comtrade_average_mf.csv
Csv that contains for each product the average value of trade quantity imported by each country, related to Comtrade yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with each partner. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: Comtrade code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## faostat_average_eu_row.csv
Csv that contains for each product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Faostat yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with EU27 and ROW. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## faostat_average_eu_row_mf.csv
Csv that contains for each product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Faostat yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with EU27 and ROW. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## comtrade_average_eu_row.csv
Csv that contains for each product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Comtrade yearly data. Data are associated to import flows.
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for partners involved into the bilateral trades with EU27 and ROW. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: Comtrade code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter (aggregation), can assume
    - "EU27"
    - "ROW"
- `partner_code`: code of data partner
- `product_code_avg_value`: average value of the total annual trade of the reporter quantity related to the specific product code
- `partner_code_percentage`: percentage value of trade quantity related to the specific partner code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code

## comtrade_average_eu_row_mf.csv
Csv that contains for each product the average value of trade quantity imported by the Europe Union (EU27) and Rest of the World (ROW), related to Comtrade yearly data. Data are associated to mirror flows (exports).
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of the trade quantity associated to the specific product by the number of years of the time period.
Percentage values are also reported for reporters involved into the bilateral trades with EU27 and ROW. The average value of the total annual trade quantity for the specific reporter, partner and product is specified. There are 8 columns:
- `product_code`: FAO code of the product
- `period`: aggregation period yyyy-yyyy
- `unit`: measurement units of the data
- `reporter_code`: code of data reporter
- `partner_code`: code of data partner (aggregation), can assume
    - "EU27"
    - "ROW"
- `product_code_avg_value`: average value of the total annual trade of the partner quantity related to the specific product code
- `reporter_code_percentage`: percentage value of trade quantity related to the specific reporter code, associated to the average value of the product code
- `avg_value`: average value of the total annual trade quantity related to the specific reporter, partner and product code