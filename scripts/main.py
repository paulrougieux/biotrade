#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Written by Eylül Kilsedar, Selene Patani and Paul Rougieux.

Start the API server point:

   ipython ~/repos/forobs/biotrade/scripts/main.py

Query the API at:

    http://127.0.0.1:8000/api/v1/reporter_query?year=2004&year=2005&year=2006&year=2007&year=2008&reporter=Indonesia
"""

from pathlib import Path
from fastapi import FastAPI, Query
from typing import List
import uvicorn
from biotrade.faostat import faostat

# Define webframework
app = FastAPI()
# Output data directory
output_dir = Path.cwd() / "scripts"


def select_products_with_largest_area(year_list, reporter, threshold=90):
    """
    Selects products with the largest harvested area based on a threshold value.

    :param (list) year_list, list of years to calculate the aggregation
    :param (str) reporter, name of country
    :param (float) threshold, percentage above which not consider single product,
        default is 90 %
    :return (DataFrame) df
    """
    # Connect to faostat db
    db = faostat.db
    # Query db
    df = db.select(table="crop_production", reporter=reporter)
    # Select only area harvested, years of list and product with code lower
    # than 1000
    df = df[
        (df["element"] == "area_harvested")
        & (df["year"].isin(year_list))
        & (df["product_code"] < 1000)
    ].reset_index(drop=True)
    # Sum the values for the same products in the given years
    # Skip nan values is true for sum by default
    df = df.groupby("product").agg({"value": "sum"}).reset_index()
    # New columns of dataframe
    df["total_value"] = df.value.sum()
    df["value_percentage"] = df["value"] / df["total_value"] * 100
    # Sort by percentage, compute the cumulative sum and shift it by one
    df.sort_values(by="value_percentage", ascending=False, inplace=True)
    # Skip nan values is True for cumsum by default
    df["cumsum"] = df.value_percentage.cumsum()
    df["cumsum_lag"] = df["cumsum"].transform("shift", fill_value=0)
    # Label harvest areas above the threshold under a product called 'Others'
    # Create a grouping variable called product_2, which will be 'Others' for
    # products above the threshold
    df["product_2"] = df["product"].where(df["cumsum_lag"] < threshold, "others")
    # Group products that are in the 'Others' category and calculate their percentage
    index = ["product_2", "total_value"]
    df = df.groupby(index).agg({"value": "sum"}).reset_index()
    df["value_percentage"] = df["value"] / df["total_value"] * 100
    return df


def create_csv_json(years, reporter, threshold=90):
    """Create JSON data for treemap visualization with D3.js.

    :param (list) years, the years to be selected
    :param (str) reporter, the reporter to be selected
    :param (float) threshold, selected products represent an area below this
        threshold percentage, it can be 0 <= threshold <= 100. Default 90%
    :return (dict) data, organized as readable json format
    """
    # Select data
    df = select_products_with_largest_area(years, reporter, threshold)
    # Dictionary creation
    data = {}
    data["name"] = reporter
    data["total_value"] = df["total_value"][0]
    data["children"] = []
    for i, row in df.iterrows():
        data_part = {}
        data_part["name"] = row["product_2"]
        data_part["value"] = row["value"]
        data_part["value_percentage"] = row["value_percentage"]
        data["children"].append(data_part)
    return data


# Define the url for api
@app.get("/api/v1/reporter_query")
# Define the api
def reporter_query(reporter: str, year: List[int] = Query(None)):
    """
    :query parameter (str) reporter, country name
    :query parameter (list of int) year, years for aggregation
    :return json data response to request
    """
    # Read query parameters and get data
    data = create_csv_json(year, reporter)
    return data


# Run the webframework
if __name__ == "__main__":
    uvicorn.run("main:app")
