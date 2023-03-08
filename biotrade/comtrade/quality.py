"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""

# Internal modules
from biotrade.comtrade import comtrade

# Third party modules
from sqlalchemy.sql import func
from sqlalchemy import select
import pandas as pd


def compare_yearly_agg_monthly():
    """
    Function which compares Comtrade yearly data with respect to the aggregation of monthly data.
    It returns a dataframe containing aggregation values which don't match

    """

    # Table with yearly data to select from raw_comtrade schema
    table = comtrade.db.tables["yearly"]
    # Left table columns to be considered before performing the join
    columns = [
        table.c.reporter_code,
        table.c.reporter,
        table.c.partner_code,
        table.c.partner,
        table.c.product_code,
        table.c.flow_code,
        table.c.flow,
        table.c.period,
        table.c.year,
    ]
    # Add quantity and monetary value columns
    table_stmt = table.select().with_only_columns(
        [
            *columns,
            table.c.net_weight.label("qnt_yearly"),
            table.c.trade_value.label("value_yearly"),
        ]
    )
    # Consider years after 2015
    table_stmt = table_stmt.where(table.c.year >= 2016)
    # Define the new table name
    yearly_selection = table_stmt.subquery().alias("yearly")
    # Table with monthly data to select from raw_comtrade schema
    table = comtrade.db.tables["monthly"]
    # Right table Columns to be considered before performing the join
    columns = [
        table.c.reporter_code,
        table.c.reporter,
        table.c.partner_code,
        table.c.partner,
        table.c.product_code,
        table.c.flow_code,
        table.c.flow,
        table.c.year,
    ]
    # Add aggregation quantity and monetary value columns
    table_stmt = table.select().with_only_columns(
        [
            *columns,
            func.sum(table.c.net_weight).label("qnt_monthly"),
            func.sum(table.c.trade_value).label("value_monthly"),
        ]
    )
    # Consider years after 2015
    table_stmt = table_stmt.where(table.c.year >= 2016)
    # Perform aggregation
    table_stmt = table_stmt.group_by(*columns)
    # Define the new table name
    monthly_selection = table_stmt.subquery().alias("monthly")
    # Full outer join to calculate the difference between results
    table_join = yearly_selection.join(
        monthly_selection,
        (yearly_selection.c.reporter_code == monthly_selection.c.reporter_code)
        & (yearly_selection.c.partner_code == monthly_selection.c.partner_code)
        & (yearly_selection.c.product_code == monthly_selection.c.product_code)
        & (yearly_selection.c.flow_code == monthly_selection.c.flow_code)
        & (yearly_selection.c.year == monthly_selection.c.year),
        isouter=True,
        full=True,
    )
    table = select(table_join).alias("table_join")
    # Final columns to be retained
    join_columns = [
        table.c.reporter_code,
        table.c.reporter,
        table.c.partner_code,
        table.c.partner,
        table.c.product_code,
        table.c.flow_code,
        table.c.flow,
        table.c.period,
        table.c.year,
        table.c.qnt_yearly,
        table.c.qnt_monthly,
        func.abs(table.c.qnt_yearly - table.c.qnt_monthly).label("abs_qnt_diff"),
        table.c.value_yearly,
        table.c.value_monthly,
        func.abs(table.c.value_yearly - table.c.value_monthly).label("abs_value_diff"),
    ]
    table_stmt = select(join_columns)
    # Return data where monthly aggregation quantities or monetary values don't match with yearly data
    table_stmt = table_stmt.where(
        (table.c.qnt_yearly != table.c.qnt_monthly)
        | ((table.c.qnt_yearly == None) & (table.c.qnt_monthly != None))
        | ((table.c.qnt_yearly != None) & (table.c.qnt_monthly == None))
        | (table.c.value_yearly != table.c.value_monthly)
        | ((table.c.value_yearly == None) & (table.c.value_monthly != None))
        | ((table.c.value_yearly != None) & (table.c.value_monthly == None))
    )
    # Return the dataframe from the query to db
    df = pd.read_sql_query(table_stmt, comtrade.db.engine)
    return df


def compare_world_agg_country(table):
    """
    Function which compares Comtrade data with partner equal to World
    and country partner aggregations. It returns a dataframe containing aggregation values which don't match with World data

    """

    # Table to select from raw_comtrade schema
    table = comtrade.db.tables[table]
    # Columns to be considered before performing the join
    columns = [
        table.c.reporter_code,
        table.c.reporter,
        table.c.product_code,
        table.c.flow_code,
        table.c.flow,
        table.c.period,
        table.c.year,
    ]
    # Add aggregation quantity and monetary value columns
    table_stmt = table.select().with_only_columns(
        [
            *columns,
            func.sum(table.c.net_weight).label("qnt_agg"),
            func.sum(table.c.trade_value).label("value_agg"),
        ]
    )
    # Consider all partners except World (partner_code = 0)
    table_stmt = table_stmt.where(table.c.partner_code > 0)
    # Perform aggregation
    table_stmt = table_stmt.group_by(*columns)
    # Define the new table name
    agg_selection = table_stmt.subquery().alias("agg")
    # Perform the same operations for partner_code = 0 (World)
    table_stmt = table.select().with_only_columns(
        [
            *columns,
            func.sum(table.c.net_weight).label("qnt_world"),
            func.sum(table.c.trade_value).label("value_world"),
        ]
    )
    table_stmt = table_stmt.where(table.c.partner_code == 0)
    table_stmt = table_stmt.group_by(*columns)
    world_selection = table_stmt.subquery().alias("world")
    # Full outer join to calculate the difference between results
    table_join = agg_selection.join(
        world_selection,
        (agg_selection.c.reporter_code == world_selection.c.reporter_code)
        & (agg_selection.c.product_code == world_selection.c.product_code)
        & (agg_selection.c.flow_code == world_selection.c.flow_code)
        & (agg_selection.c.period == world_selection.c.period),
        isouter=True,
        full=True,
    )
    table = select(table_join).alias("table_join")
    # Final columns to be retained
    join_columns = [
        table.c.reporter_code,
        table.c.reporter,
        table.c.product_code,
        table.c.flow_code,
        table.c.flow,
        table.c.period,
        table.c.year,
        table.c.qnt_agg,
        table.c.qnt_world,
        func.abs(table.c.qnt_agg - table.c.qnt_world).label("abs_qnt_diff"),
        table.c.value_agg,
        table.c.value_world,
        func.abs(table.c.value_agg - table.c.value_world).label("abs_value_diff"),
    ]
    table_stmt = select(join_columns)
    # Return data where aggregation quantities or monetary values don't match with World data provided by Comtrade
    table_stmt = table_stmt.where(
        (table.c.qnt_agg != table.c.qnt_world)
        | ((table.c.qnt_agg == None) & (table.c.qnt_world != None))
        | ((table.c.qnt_agg != None) & (table.c.qnt_world == None))
        | (table.c.value_agg != table.c.value_world)
        | ((table.c.value_agg == None) & (table.c.value_world != None))
        | ((table.c.value_agg != None) & (table.c.value_world == None))
    )
    # Return the dataframe from the query to db
    df = pd.read_sql_query(table_stmt, comtrade.db.engine)
    return df
