"""
Written by Selene Patani.

Functions to return time series change detections.
"""

import pandas as pd
import numpy as np
from scipy import optimize, stats
import matplotlib.pyplot as plt


def obj_function(breakpoints, x, y, num_breakpoints, fcache):
    """
    Function which computes errors of the segmented regressions
    based on the mean of R2 with respect to the number of break points.
    Then objective function is calculated recentered the total error around 1.
    :param (np.array) breakpoints, the location of breakpoints
    :param (float) x, values of independent variable
    :param (float) y, values of dependent variable
    :param (integer) num_breakpoints, number of breaks equal to segments - 1
    :param (dictionary) fcache, values of the objective function for each evaluated
        location
    :return (dictionary) fcache updated
    """
    # Reorder breakpoint locations into a tuple
    breakpoints = tuple(map(int, sorted(breakpoints)))
    # Calculate obj function if not already done for the specific breakpoint location
    if breakpoints not in fcache:
        # Initialization
        total_error = 0
        # Find the linear regression coefficients for the segments cut by breakpoint locations
        for f, xi, yi in find_best_piecewise_polynomial(breakpoints, x, y):
            # Calculate the total error based on R2 and number of breakpoints
            total_error += (f.rvalue) ** 2 / (num_breakpoints + 1)
        # Recenter the obj function with respect to 1
        fcache[breakpoints] = abs(total_error - 1)
    return fcache[breakpoints]


def find_best_piecewise_polynomial(breakpoints, x, y):
    """
    Function with computes the linear regression statistics for the breakpoint locations
    :param (np.array) breakpoints, the location of breakpoints
    :param (float) x, values of independent variable
    :param (float) y, values of dependent variable
    :return (list) result, containing statistics of the linear regression and values of the independent and
    dependent piece variables
    """
    # If the breakpoint is 1 transform it into a tuple
    if isinstance(breakpoints, np.floating):
        breakpoints = (int(breakpoints),)
    # Reorder breakpoint locations into a tuple
    else:
        breakpoints = tuple(map(int, sorted(breakpoints)))
    # Split x and y vectors into num_breakpoints +1 segments
    xs = np.split(x, breakpoints)
    ys = np.split(y, breakpoints)
    # Initialization
    result = []
    # Calculate the linear regression coefficients and store them into the result list
    for xi, yi in zip(xs, ys):
        # Do not consider linear regressions with less than 7 points
        if len(xi) < 7:
            continue
        # Compute the regression and store the results for each segment
        f = stats.linregress(xi, yi)
        result.append([f, xi, yi])
    return result


def relative_change(df, years=5, last_value=True):
    """
    Calculate the relative change of a quantity with respect to
    values assumed by that quantity in the n previous years for
    production and trade data.
    Relative change of a value is calculated as:
        (value of the reference year - mean of values of previous n years) / mean of values of previous n years * 100

    :param (DataFrame) df, dataframe containing the quantity with respect to calculate the relative change in time
    :param (int) years, temporal window over calculating the relative change (default = 5)
    :param (boolean) last_value, if True returns only values related to the most recent year (default = True)

    :return (DataFrame) df_rel_change, dataframe which contains a new column, "relative_change" calculation

    As example, compute the relative change for soy trade products from Brazil as reporter to Europe and
    rest of the world as partners.
    Import dependencies
        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> from biotrade.common.time_series import relative_change
    Select soy trade products with Brazil as reporter
        >>> soy_trade = faostat.db.select(table="crop_trade",
                reporter="Brazil", product="soy")
    Aggregate partners as Europe (eu) and rest of the World (row)
        >>> soy_trade_agg = agg_trade_eu_row(soy_trade)
    Calculate the relative change of values in time (years = 5 as default) for the most recent year
        >>> soy_trade_relative_change = relative_change(soy_trade_agg, years= 5, last_value = True)
    """
    # Define group columns
    groupby_column_list = ["reporter_code", "product_code", "element", "unit"]
    # Trade data contain information also regarding partners
    if "partner" in df.columns:
        groupby_column_list.append("partner")
    # Define groups with respect to calculate the relative change of values in time
    df_groups = df.groupby(groupby_column_list)
    # Allocate df reshaped to return
    df_rel_change = pd.DataFrame()
    # Column list to perform calculations
    column_list_calc = []
    # Add columns with respect to the number of years to calculate the relative change
    for year in range(1, years + 1):
        column_list_calc.append(f"value_{year}_year_before")
    # Colum to perform the mean value of previous years
    column_list_calc.append("mean_previous_years")
    # Loop on each group
    for key in df_groups.groups.keys():
        # Select data related to the specific group
        df_key = df_groups.get_group(key)
        # Sort data from the most recent year
        df_key = df_key.sort_values(by="year", ascending=False)
        # Calculate values related to most recent year at disposal
        if last_value:
            df_key = df_key[0 : years + 1]
        # Add column to dataframe
        for idx, column in enumerate(column_list_calc):
            # The last column is the mean of the previous year values
            if column == "mean_previous_years":
                df_key[column] = df_key[column_list_calc[:-1]].mean(
                    axis=1, skipna=False
                )
                break
            # Shift value column up with respect to the number of years considered
            df_key[column] = df_key["value"].shift(-(idx) - 1)
        # Finally calculate the relative change column
        df_key["relative_change"] = (
            (df_key["value"] - df_key["mean_previous_years"])
            / df_key["mean_previous_years"]
            * 100
        )
        # 0/0 and float/0 treated as NaN
        df_key["relative_change"][np.isinf(df_key["relative_change"])] = np.nan
        # Drop columns added for the calculation
        df_key = df_key.drop(column_list_calc, axis=1)
        # Return values related to most recent year at disposal
        if last_value:
            df_key = df_key[0:1]
        # Add the group to the final dataframe to return
        df_rel_change = df_rel_change.append(df_key, ignore_index=True)
    # Final dataframe with the new column relative_change
    return df_rel_change


def segmented_regression(df, plot=False, last_value=True):
    """
    Calculate the linear regression statistics of the segmented regression
    based on the optimization of the objective function built on R2 results

    :param (DataFrame) df, dataframe containing the time series with respect to calculate the segmented regressions
    :param (boolean) plot, if True return plot of the regressions (default = False)
    :param (boolean) last_value, if True returns only values related to the most recent year (default = True)

    :return (DataFrame) df_segmented_regression, dataframe which contains new columns related to
    linear regression calculations

    As example, compute the segmented regression for soybean exports from Brazil as reporter to Europe and
    rest of the world as partners.
    Import dependencies
        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> from biotrade.common.time_series import segmented_regression
        >>> import matplotlib.pyplot as plt

    Select soybean trade products with Brazil as reporter
        >>> soybean_trade = faostat.db.select(table="crop_trade",
                reporter="Brazil", product_code=236)
    Aggregate partners as Europe (eu) and rest of the World (row)
        >>> soybean_trade_agg = agg_trade_eu_row(soybean_trade)
    Select export quantity
        >>> soybean_exp_agg = soybean_trade_agg[soybean_trade_agg["element"] == "export_quantity"]
    Calculate the segmented regression of values returning the most recent year
        >>> soybean_exp_regression = segmented_regression(soybean_exp_agg, plot = True, last_value = True)
        >>> plt.show()
    """
    # Define group columns
    groupby_column_list = ["reporter_code", "product_code", "element", "unit"]
    # Trade data contain information also regarding partners
    if "partner" in df.columns:
        groupby_column_list.append("partner")
    # Define groups with respect to calculate the segmented regressions
    df_groups = df.groupby(groupby_column_list)
    # Allocate df reshaped to return
    df_segmented_regression = pd.DataFrame()
    # Loop on each group
    for key in df_groups.groups.keys():
        # Select data related to the specific group
        df_key = df_groups.get_group(key)
        # Create arrays of independent (column "year") and dependent (colum "values") variables
        x = np.array(df_key["year"].values.tolist(), dtype=float)
        y = np.array(df_key["value"].values.tolist())
        # Pre allocation
        best_breakpoints = ()
        # Loop over the number of breakpoints (max number of breakpoints is len(x)/7 -1)
        # to check the best objective function result
        for num_breakpoints in range(0, round(len(x) / 7)):
            # If no breakpoints, no need to optimize the breakpoint location
            if num_breakpoints == 0:
                # Fake position
                breakpoints_pos = (0,)
                # Calculate the objective function for the entire array
                breakpoints_result = obj_function((0,), x, y, num_breakpoints, {})
                # Store results
                breakpoints = (breakpoints_pos, breakpoints_result)
            # Call the optimization function optimize.brute which look at
            # a global minimum, moving 1 position at a time
            else:
                # Define the grid
                grid = [slice(1, len(x), 1)] * num_breakpoints
                # Return the optimize breakpoint location function
                breakpoints = optimize.brute(
                    obj_function,
                    grid,
                    args=(x, y, num_breakpoints, {}),
                    full_output=True,
                    finish=None,
                )
            # Choose the best breakpoint locations considering also different number of breakpoints
            if best_breakpoints == ():
                best_breakpoints = breakpoints
            elif breakpoints[1] < best_breakpoints[1]:
                best_breakpoints = breakpoints
        # If plot is True plot the x-y scattered values
        if plot:
            plt.figure()
            plt.scatter(x, y, c="blue", s=50)
        # Add new columns to df reporting the linear regression statistics
        for f, xi, yi in find_best_piecewise_polynomial(best_breakpoints[0], x, y):
            df_key.loc[
                (df_key["year"] >= xi.min()) & (df_key["year"] <= xi.max()),
                [
                    "slope",
                    "intercept",
                    "rsquared",
                    "pvalue",
                    "stderr_slope",
                    "stderr_intercept",
                ],
            ] = [
                f.slope,
                f.intercept,
                f.rvalue ** 2,
                f.pvalue,
                f.stderr,
                f.intercept_stderr,
            ]
            # If plot is True plot the x-y estimated values of the segmented regression
            if plot:
                x_interval = np.array([xi.min(), xi.max()])
                y_interval = f.slope * x_interval + f.intercept
                plt.plot(x_interval, y_interval, "ro-")
        # Return values related to most recent year at disposal
        if last_value:
            df_key = df_key.sort_values(by="year", ascending=False)[0:1]
        # Add the group to the final dataframe to return
        df_segmented_regression = df_segmented_regression.append(
            df_key, ignore_index=True
        )
    # Final dataframe with the new column statistic columns
    return df_segmented_regression
