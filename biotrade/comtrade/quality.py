"""
Written by Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

"""

# Internal modules
from biotrade.comtrade import comtrade

# Third party modules
from sqlalchemy.sql import func
from sqlalchemy import select
import pandas as pd


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
    # Outer join to calculate the difference between results
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

