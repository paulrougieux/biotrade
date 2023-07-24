"""Common functions suitable for many data sources

For example the following functions are common to two or more data sources:

- `biotrade.common.compare.transform_comtrade_using_faostat_codes` transforms
Comtrade data into a format compatible with FAOSTAT data for comparison
purposes.

- `biotrade.common.aggregate.agg_trade_eu_row` aggregate trade to EU
and Rest of the World on the reporter side, the partner side or both.

- `biotrade.common.aggregate.nlargest` return the n largest producers or n largest
trade partners of a product with. Rows are sorted by the first of the
value_vars and slicing takes the first n rows in each slice group.

- `biotrade.common.update.cron_quarterly` and
`biotrade.common.update.cron_bi_weekly` define cron jobs which will
automatically update datasets every three months or every two weeks.

"""
