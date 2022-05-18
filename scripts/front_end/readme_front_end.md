# METADATA
Table names and metadata of the web platform output scripts

## reporter_list.csv
Csv that contains the list of all reporters. There are 2 columns:
- `reporter_code`: ISO3 code of data reporter
- `reporter_name`: name of the reporter

## product_list.csv
Csv that contains the list of all products. There are 3 columns:
- `product_code`: FAO code of the product
- `product_name`: name of the product
- `key_product_flag`: 1 if the product is a key product; 0 otherwise

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
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## production_annual_variation.csv
Csv that contains annual data of production for each of the key products and the related reporters. There are 5 columns:
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `period`: time of data
- `value`: data registered
- `unit`: measurement units of the data

## harvested_area_average.csv
Csv that contains for each product (not only key products) the average value of the harvested area across reporters and for each reporter the average value of the harvested area across products. 
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of harvested area associated to the product (or 
reporter) by the number of years of the time period.
Percentage values are also reported for products and reporters. The percentage of a product (or reporter) has to be comprised in the 95% of the descendent cumulative sum of harvested area for a specific reporter (or product). The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 8 columns:
- `period`: aggregation period yyyy-yyyy
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `average_value_reporter`: average value of harvested area related to the specific reporter code, across all the associated products
- `value_percentage_product`: percentage value of harvested area related to the specific product code, across all products of the reporter code 
- `average_value_product`: average value of harvested area related to the specific product code, across all the associated reporters
- `value_percentage_reporter`: percentage value of harvested area related to the specific reporter code, across all reporters of the product code
- `unit`: measurement units of the data

## production_average.csv
Csv that contains for each product (not only key products) the average value of the production across reporters and for each reporter the average value of the production across products. 
Aggregation is generally performed, except the last year where data are not aggregated.
The average value is obtained dividing the sum over the time period of production associated to the product (or 
reporter) by the number of years of the time period.
Percentage values are also reported for products and reporters. The percentage of a product (or reporter) has to be comprised in the 95% of the descendent cumulative sum of production for a specific reporter (or product). The remaining 5% of percentages is grouped into the generic category "Others", because they have negligible percentages to be further distinguished. There are 8 columns:
- `period`: aggregation period yyyy-yyyy
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `average_value_reporter`: average value of production related to the specific reporter code, across all the associated products
- `value_percentage_product`: percentage value of production related to the specific product code, across all products of the reporter code 
- `average_value_product`: average value of production related to the specific product code, across all the associated reporters
- `value_percentage_reporter`: percentage value of production related to the specific reporter code, across all reporters of the product code
- `unit`: measurement units of the data

## harvested_area_trends.csv
Csv that contains trends of harvested area associated to the key products. For each reporter and each key product, the time series after 1986 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of harvested area associated to the most recent year with respect to the average value of a certain aggregation period and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `period`: most recent year of data analyzed
- `period_change`: aggregation period considered to obtain the relative and absolute changes of harvested area yyyy-yyyy
- `period_regression`: period of the most recent time series segment, obtained from the segmented regression analysis. The Mann-Kendall test is performed over the data included in this period yyyy-yyyy
- `relative_change`: harvested area (associated to the specific product and reporter) of the most recent year minus the average value over the period change, divided by this last value
- `absolute_change`: harvested area (associated to the specific product and reporter) of the most recent year minus the average value over the period change
- `mk_slope`: annual variation (slope) obtained from the Mann-Kendall test
- `mk_significance_flag`: 1 means that the statistical test is valid (significance level = 0.05), 0 otherwise
- `unit`: measurement units of the data

## production_trends.csv
Csv that contains trends of production associated to the key products. For each reporter and each key product, the time series after 1986 is analyzed to obtain indicators for the most recent year. The indicators are: the absolute and relative changes of production associated to the most recent year with respect to the average value of a certain aggregation period and the results of the Mann-Kendall trend test performed on the segmented regression analysis. There are 10 columns:
- `reporter_code`: ISO3 code of data reporter
- `product_code`: FAO code of the product
- `period`: most recent year of data analyzed
- `period_change`: aggregation period considered to obtain the relative and absolute changes of production yyyy-yyyy
- `period_regression`: period of the most recent time series segment, obtained from the segmented regression analysis. The Mann-Kendall test is performed over the data included in this period yyyy-yyyy
- `relative_change`: production (associated to the specific product and reporter) of the most recent year minus the average value over the period change, divided by this last value
- `absolute_change`: production (associated to the specific product and reporter) of the most recent year minus the average value over the period change
- `mk_slope`: annual variation (slope) obtained from the Mann-Kendall test
- `mk_significance_flag`: 1 means that the statistical test is valid (significance level = 0.05), 0 otherwise
- `unit`: measurement units of the data