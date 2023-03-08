#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""

# Internal modules
from biotrade.faostat import faostat

# Third party modules
from sqlalchemy.sql import func
from sqlalchemy import select
import pandas as pd


def compare_crop_production_harvested_area_world_agg_country(
    drop_identical=False, product_code=None
):
    """
    Function which compares Faostat crop production and harvested area with partner equal to World
    and country partner aggregations. It returns a dataframe containing the difference between aggregation values and World data reported by Faostat

    :param drop_identical (boolean), if True drop rows where the aggregation value match with the Word value reported by Faostat, otherwise it keeps all the rows
    """
    if isinstance(product_code, (int, str)):
        product_code = [product_code]
    # Crop production table to select from raw_faostat schema
    table = faostat.db.tables["crop_production"]
    # Columns to be considered before performing the join
    columns = [
        table.c.product_code,
        table.c.product,
        table.c.element_code,
        table.c.element,
        table.c.unit,
        table.c.period,
        table.c.year,
    ]
    # Add aggregation quantity column
    table_stmt = table.select().with_only_columns(
        [*columns, func.sum(table.c.value).label("qnt_agg")]
    )
    # Consider all reporters excluding China, regional, subcontinent and continent aggregations (reporter_code < 300)
    table_stmt = table_stmt.where(table.c.reporter_code < 300)
    # Consider specific product codes if defined
    if product_code is not None:
        table_stmt = table_stmt.where(table.c.product_code.in_(product_code))
    # Perform aggregation
    table_stmt = table_stmt.group_by(*columns)
    # Define the new table name
    agg_selection = table_stmt.subquery().alias("agg")
    # Perform the same operations for reporter_code = 5000 (World)
    table_stmt = table.select().with_only_columns(
        [
            *columns,
            func.sum(table.c.value).label("qnt_world"),
        ]
    )
    table_stmt = table_stmt.where(table.c.reporter_code == 5000)
    # Consider specific product codes if defined
    if product_code is not None:
        table_stmt = table_stmt.where(table.c.product_code.in_(product_code))
    table_stmt = table_stmt.group_by(*columns)
    world_selection = table_stmt.subquery().alias("world")
    # Full outer join to calculate the difference between results
    table_join = agg_selection.join(
        world_selection,
        (agg_selection.c.product_code == world_selection.c.product_code)
        & (agg_selection.c.element_code == world_selection.c.element_code)
        & (agg_selection.c.unit == world_selection.c.unit)
        & (agg_selection.c.period == world_selection.c.period),
        isouter=True,
        full=True,
    )
    table = select(table_join).alias("table_join")
    # Final columns to be retained
    join_columns = [
        table.c.product_code,
        table.c.product,
        table.c.element_code,
        table.c.element,
        table.c.unit,
        table.c.period,
        table.c.year,
        table.c.qnt_agg,
        table.c.qnt_world,
        (table.c.qnt_agg - table.c.qnt_world).label("qnt_diff"),
    ]
    table_stmt = select(join_columns)
    # Return all crop production and harvested area data
    table_stmt = table_stmt.where(table.c.unit.in_(["Head", "1000 No", "tonnes", "ha"]))
    if drop_identical:
        # Return crop production and harvested area data where aggregation quantities don't match with World data provided by Faostat
        # Null values are not counted in comparison with standard sql, added a separately statement
        table_stmt = table_stmt.where(
            (table.c.qnt_agg != table.c.qnt_world)
            | ((table.c.qnt_agg is None) & (table.c.qnt_world is not None))
            | ((table.c.qnt_agg is not None) & (table.c.qnt_world is None))
        )
    # Return the dataframe from the query to db
    df = pd.read_sql_query(table_stmt, faostat.db.engine)
    return df


def compare_prod_agg_to_parts(df, prod_aggregate, prod_constituents):
    """Compare aggregate products to the sum of their constituents

    :param DataFrame df: FAOSTAT production data in long format
    :param str prod_aggregate: Name of the aggregate product
    :param list prod_constituents: Names of the product constituents

    Return a reshaped data frame in wider format with aggregates and their constituents in columns.

    For more information, see the FAOSTAT forest products classification and
    standards at https://www.fao.org/forestry/statistics/80572/en/
    in particular the data structure chart
    https://www.fao.org/forestry/49962-0f43c0da7039a611aa884b3c6c642f4ac.pdf
    explains how the different products and aggregates relate to each other.

    For example compare roundwood to the sum of industrial roundwood and fuel
    wood. In the complete forestry production dataset (including production,
    import and export values and quantities).

    >>> from biotrade.faostat import faostat
    >>> from biotrade.faostat.quality import compare_prod_agg_to_parts
    >>> fp = faostat.pump.read_df("forestry_production")
    >>> rwd = compare_prod_agg_to_parts(
    >>>     fp, "roundwood", ["industrial_roundwood", "wood_fuel"]
    >>> )
    >>> assert all(rwd["diff"] == 0)

    """
    prod_together = [prod_aggregate] + prod_constituents
    index = ["reporter", "element", "unit", "year"]
    df = (
        df.query("product in @prod_together")
        .pivot(index=index, columns="product", values="value")
        .fillna(0)
    )
    df[prod_aggregate + "_agg"] = df.drop(columns=prod_aggregate).sum(axis=1)
    df["diff"] = df[prod_aggregate] - df[prod_aggregate + "_agg"]
    return df


def check_prod_agg_and_parts(df, prod_aggregate, prod_constituents):
    """Check that the product aggregate corresponds to the sum of its constituents

    This function has the same arguments as compare_prod_agg_to_parts. In case
    of an error, it gives the function call necessary to show the comparison
    data frame for further investigation.
    """
    output_df = compare_prod_agg_to_parts(
        df, prod_aggregate=prod_aggregate, prod_constituents=prod_constituents
    )
    if all(output_df["diff"] == 0):
        msg = f"OK: the product aggregate {prod_aggregate} "
        msg += f"does correspond to the sum of its constituents:\n {prod_constituents}"
        print(msg)
    else:
        msg = f"The product aggregate {prod_aggregate} "
        msg += (
            f"does not correspond to the sum of its constituents:\n {prod_constituents}"
        )
        msg += "\nLoad the data frame with:\n"
        msg += "from biotrade.faostat.quality import compare_prod_agg_to_parts\n"
        msg += f"compare_prod_agg_to_parts(your_dataframe, '{prod_aggregate}', {prod_constituents})"
        raise ValueError(msg)
