"""
Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

Functions to return time series change detections and associated plots.
"""

import math
import pandas as pd
import numpy as np
from scipy import optimize, stats
import matplotlib.pyplot as plt
import pymannkendall as mk
from collections import OrderedDict
from functools import partial
from multiprocessing import Pool


def plot_relative_absolute_change(df):
    """
    Function to plot relative and absolute change values (see relative_absolute_change function for the calculations)
    for each group of data
    :param (DataFrame) df, the output of relative_absolute_change function

    For example, select production quantity of palm oil from 1986 with Malaysia as reporter, compute the change values with respect
    to the previous five years and plot results

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.time_series import relative_absolute_change, plot_relative_absolute_change
        >>> palm_oil_data = faostat.db.select(
        >>>     table="crop_production", reporter="Malaysia", product_code=257
        >>> )
        >>> palm_oil_prod = palm_oil_data[
        >>>     (palm_oil_data["element"] == "production") & (palm_oil_data["year"] > 1986)
        >>> ]
        >>> df_change = relative_absolute_change(palm_oil_prod, last_value=False)
        >>> plot_relative_absolute_change(df_change)

    """
    # Group of columns to be considered
    group_data_pivot_table = [
        "reporter",
        "unit",
        "product_code",
    ]
    # Depending on the df, other columns to be considered in order to group the data
    add_group_data_column = [
        "product",
        "partner",
        "element",
        "flow",
        "unit_code",
    ]
    # Select for grouping the data only columns contained into the df
    for column in df:
        if column in add_group_data_column:
            # If Comtrade db, unit code and unit are nan and have to be removed
            if column == "unit_code":
                group_data_pivot_table.remove("unit")
                continue
            group_data_pivot_table.append(column)
    # Reshape df to plot
    df_reshape = df.pivot_table(
        index="year",
        columns=group_data_pivot_table,
        values=["relative_change", "absolute_change"],
    )
    # Plot relative change values
    ax = df_reshape["relative_change"].plot.bar(
        figsize=(20, 10),
        fontsize=16,
    )
    ax.set_xlabel("Time [y]", fontsize=20)
    ax.set_ylabel("[%]", fontsize=20)
    if len(df["year_range_lower"].unique()) > 1:
        add_title = " (wrt average of 5 previous years)"
    else:
        add_title = f" (wrt average of {df['year_range_lower'].unique()[0]}-{df['year_range_upper'].unique()[0]})"
    ax.set_title("Relative change" + add_title, fontsize=20)
    # Plot absolute change values
    ax = df_reshape["absolute_change"].plot.line(
        figsize=(20, 10),
        fontsize=16,
    )
    ax.set_xlabel("Time [y]", fontsize=20)
    if "unit" in group_data_pivot_table:
        ax.set_ylabel(
            f"[{'/'.join(df['unit'].unique().tolist())}]", fontsize=20
        )
    ax.set_title("Absolute change" + add_title, fontsize=20)
    # Show plot
    plt.show()


def plot_segmented_regression(df):
    """
    Function to plot segments defined by the linear regression interpolant (see segmented_regression function for the calculations)
    for each group of data
    :param (DataFrame) df, the output of segmented_regression function

    For example, select production quantity of palm oil from 1986 with Malaysia as reporter, compute the segmented regression analysis and plot results

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.time_series import segmented_regression, plot_segmented_regression
        >>> palm_oil_data = faostat.db.select(
        >>>     table="crop_production", reporter="Malaysia", product_code=257
        >>> )
        >>> palm_oil_prod = palm_oil_data[
        >>>     (palm_oil_data["element"] == "production") & (palm_oil_data["year"] > 1986)
        >>> ]
        >>> df_segment_reg = segmented_regression(palm_oil_prod, last_value=False, function="R2")
        >>> plot_segmented_regression(df_segment_reg)

    """
    # Define value column name
    if "value" in df.columns:
        # Value column of faostat db
        value_column = "value"
    elif "net_weight" in df.columns:
        # Value column is called "net_weight" in comtrade db
        value_column = "net_weight"
    # Define group columns
    groupby_column_list = ["reporter", "product_code", "unit"]
    # Depending on the db tables, different columns can be used to group the dataframe
    add_columns = ["partner", "element", "flow", "unit_code"]
    # Check if columns are present in the original dataframe
    for column in add_columns:
        if column in df.columns:
            # For comtrade db, do not consider unit and unit_code columns which are always nan
            if column == "unit_code":
                groupby_column_list.remove("unit")
                continue
            groupby_column_list.append(column)
    # Define groups with respect to calculate the segmented regressions
    df_groups = df.groupby(groupby_column_list)
    # Extract the information of significance level imposed and the type of obj function
    alpha = df["significance_level"].unique()[0]
    function = df["obj_function"].unique()[0]
    # Loop on each group to plot
    for key in df_groups.groups.keys():
        # Select data related to the specific group
        df_key = df_groups.get_group(key).sort_values(by="year")
        # Create arrays of independent (column "year") and dependent (colum "values") variables
        x = np.array(df_key["year"].values.tolist())
        y = np.array(df_key[value_column].values.tolist())
        # Plot the x-y scattered values
        plt.figure(figsize=(20, 10))
        plt.rc("font", size=16)
        plt.scatter(x, y, c="blue", s=50, label="Time series")
        plt.xlabel("Time [y]", fontsize=20)
        if "partner" in df_key:
            title = f"Reporter {df_key['reporter'].unique()[0]} - Partner {df_key['partner'].unique()[0]} ({function})"
        # No partner, only production
        else:
            title = f"Reporter {df_key['reporter'].unique()[0]} ({function})"
        plt.title(
            title,
            fontsize=20,
        )
        # Faostat tables
        if "element" in df_key:
            ylabel = f"Product {df_key['product'].unique()[0]} ({df_key['element'].unique()[0]}) [{df_key['unit'].unique()[0]}]"
        # Comtrade tables
        else:
            ylabel = f"Product {df_key['product'].unique()[0]} ({df_key['flow'].unique()[0]})"
        plt.ylabel(
            ylabel,
            fontsize=20,
        )
        # Plot segments
        for year_range in (
            df_key[["year_range_lower", "year_range_upper"]]
            .drop_duplicates()
            .values
        ):
            # Extract segment dataframe
            df_key_segment = df_key[
                (df_key["year"] >= year_range[0])
                & (df_key["year"] <= year_range[1])
            ]
            # Extract information to plot
            pvalue_segment = df_key_segment["pvalue"].unique()[0]
            lower_year_segment = df_key_segment["year_range_lower"].unique()[0]
            upper_year_segment = df_key_segment["year_range_upper"].unique()[0]
            slope_segment = df_key_segment["slope"].unique()[0]
            intercept_segment = df_key_segment["intercept"].unique()[0]
            # Plot only statically significant results below the significance level for the segmented regression
            if pvalue_segment < alpha:
                x_interval = np.array([lower_year_segment, upper_year_segment])
                y_interval = slope_segment * x_interval + intercept_segment
                plt.plot(
                    x_interval, y_interval, "ro-", label="Segmented regression"
                )
            # For the last segment the Mann Kendall test has been performed
            if upper_year_segment == x.max():
                mk_ha_segment = df_key_segment["mk_ha_test"].unique()[0]
                # If the test succeeded, plot the associated results
                if mk_ha_segment:
                    mk_slope_segment = df_key_segment["mk_slope"].unique()[0]
                    mk_intercept_segment = df_key_segment[
                        "mk_intercept"
                    ].unique()[0]
                    x_mk_interval = np.array(
                        [lower_year_segment, upper_year_segment]
                    )
                    y_mk_interval = (
                        mk_slope_segment * x_mk_interval + mk_intercept_segment
                    )
                    plt.plot(
                        x_mk_interval,
                        y_mk_interval,
                        "o-",
                        color="orange",
                        label="Theil–Sen estimator",
                    )
            # Do not plot vertical line if it is the last value of the time series
            else:
                plt.axvline(
                    x=upper_year_segment + 0.5,
                    linestyle="--",
                    color="k",
                    label="Breakpoint",
                )
        # Create legend without repetitions
        handles, labels = plt.gca().get_legend_handles_labels()
        by_label = OrderedDict(zip(labels, handles))
        plt.legend(by_label.values(), by_label.keys())

    # Show plot
    plt.show()


def obj_function(
    breakpoints, x, y, num_breakpoints, function, min_data_points, fcache
):
    """
    Function which computes the obj of the segmented regressions
    based on the mean of coefficient of determination (R2) with respect to the number of break points or based on the Residual Sum of Squares (RSS),
    depending on user choice.
    :param (np.array) breakpoints, the location of breakpoints
    :param (float) x, values of independent variable
    :param (float) y, values of dependent variable
    :param (string) function, the objective to calculate could be "R2" or "RSS"
    :param (integer) num_breakpoints, number of breaks, in other terms nr. segments - 1
    :param (string) function, the objective to calculate could be "R2" or "RSS", i.e. coefficient of determination based or
        residual sum of squares based (default = "RSS")
    :param (int) min_data_points, minimum nr of data for each segment of the linear regression (default = 7)
    :param (dictionary) fcache, values of the objective function for each evaluated
        location

    :return (dictionary) fcache updated
    """
    # Reorder breakpoint locations into a tuple
    breakpoints = tuple(map(int, sorted(breakpoints)))
    # Calculate obj function if not already done for the specific breakpoint location
    if breakpoints not in fcache:
        # Calculate the linear regression coefficients for the segments cut by breakpoint locations
        result = find_best_piecewise_polynomial(
            breakpoints, x, y, min_data_points
        )
        # Segments with more than 6 point
        if result:
            # Initialization
            obj = 0
            # Loop on each segment to obtain the total obj value
            for f, xi, yi in result:
                # Calculate the obj function as Residual Sum of Squares (RSS)
                if function == "RSS":
                    obj += np.sum((yi - (f.slope * xi + f.intercept)) ** 2)
                # Calculate the obj function based on coefficient of determination (R2) averaged over the number of breakpoints
                # Take the opposite value because obj needs to be minimize
                elif function == "R2":
                    if np.sum((yi - yi.mean()) ** 2) > 0:
                        obj += -(
                            1
                            - (np.sum((yi - (f.slope * xi + f.intercept)) ** 2))
                            / (np.sum((yi - yi.mean()) ** 2))
                        ) / (num_breakpoints + 1)
                    # If mean of values is 0, do not divide by it
                    else:
                        obj += -(
                            1
                            - (np.sum((yi - (f.slope * xi + f.intercept)) ** 2))
                        ) / (num_breakpoints + 1)
        # Penalize segments with less than min_data_points
        else:
            obj = np.inf
        fcache[breakpoints] = obj
    return fcache[breakpoints]


def find_best_piecewise_polynomial(
    breakpoints,
    x,
    y,
    min_data_points,
):
    """
    Function with computes the linear regression statistics for the breakpoint locations
    :param (np.array) breakpoints, the location of breakpoints
    :param (float) x, values of independent variable
    :param (float) y, values of dependent variable
    :param (int) min_data_points, minimum nr of data for each segment of the linear regression (default = 7)

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
        # Do not consider empty arrays
        if not xi.size:
            continue
        # Do not consider linear regressions with less than minimum number of points argument
        elif 0 < len(xi) < min_data_points:
            # Break the loop and penalize the result putting an empty list
            result = []
            break
        # Compute the regression and store the results for each segment
        # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.linregress.html
        f = stats.linregress(xi, yi)
        result.append([f, xi, yi])
    return result


def optimize_breakpoints(
    df, value_column, min_data_points, alpha, last_value, function
):
    """
    It allows to find the best number of breakpoints for a given time series and performs statistical tests
    on the optimized process

    :param (DataFrame) df, dataframe containing 1 time series for the optimization process
    :param (string) value_column, name of the column containing the data
    :param (int) min_data_points, minimum nr of data for each segment of the linear regression
    :param (float) alpha, significance level of statistical tests
    :param (boolean) last_value, if True returns only values related to the most recent year
    :param (string) function, the objective to calculate could be "R2" or "RSS", i.e. coefficient of determination or
        residual sum of squares

    :return (DataFrame) df, dataframe which contains new columns related to
    linear regression calculations for the given time series
    """
    # Avoid unexpected behavior of pandas apply function on df
    df = df.copy()
    # Select data related to the specific group dropping nan values
    df = df.sort_values(by="year").dropna(subset=value_column)
    # Skip segmented analysis for groups with nr of data less than minimum required
    if len(df) >= min_data_points:
        # Create arrays of independent (column "year") and dependent (colum "values") variables
        x = np.array(df["year"].values.tolist(), dtype=float)
        y = np.array(df[value_column].values.tolist())
        # Pre allocation
        best_breakpoints = ()
        # Loop over the number of breakpoints (max number of breakpoints is floor(len(x)/min_data_points) -1)
        # to check the best objective function result
        for num_breakpoints in range(0, math.floor(len(x) / min_data_points)):
            # If no breakpoints, no need to optimize the breakpoint location
            if num_breakpoints == 0:
                # Fake position
                breakpoints_pos = (0,)
                # Calculate the objective function for the entire array
                breakpoints_result = obj_function(
                    (0,), x, y, num_breakpoints, function, min_data_points, {}
                )
                # Store results
                breakpoints = (breakpoints_pos, breakpoints_result)
            # Call the optimization function optimize.brute which look at
            # a global minimum, moving 1 position at a time
            else:
                # Define the grid
                grid = [slice(1, len(x), 1)] * num_breakpoints
                # Return the optimize breakpoint location function
                # https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.brute.html
                breakpoints = optimize.brute(
                    obj_function,
                    grid,
                    args=(x, y, num_breakpoints, function, min_data_points, {}),
                    full_output=True,
                    finish=None,
                )
            # Choose the best breakpoint locations considering also different number of breakpoints
            if best_breakpoints == ():
                best_breakpoints = breakpoints
            elif breakpoints[1] < best_breakpoints[1]:
                best_breakpoints = breakpoints
        # Add new columns to df reporting the linear regression statistics
        for f, xi, yi in find_best_piecewise_polynomial(
            best_breakpoints[0], x, y, min_data_points
        ):
            df.loc[
                (df["year"] >= xi.min()) & (df["year"] <= xi.max()),
                [
                    "slope",
                    "intercept",
                    "rsquared",
                    "pvalue",
                    "stderr_slope",
                    "stderr_intercept",
                    "year_range_lower",
                    "year_range_upper",
                ],
            ] = [
                f.slope,
                f.intercept,
                f.rvalue**2,
                f.pvalue,
                f.stderr,
                f.intercept_stderr,
                xi.min(),
                xi.max(),
            ]
            if xi.max() == x.max():
                # Significance level alpha (0.05 is the default)
                mk_results = mk.original_test(yi, alpha)
                # The estimate of slope is divided by the mean of time lags
                mk_results = mk_results._replace(
                    slope=mk_results.slope / np.mean(np.diff(xi))
                )
                # The estimate of the intercept is calculated by use of the Conover (1980) equation
                mk_results = mk_results._replace(
                    intercept=np.median(yi) - np.median(xi) * mk_results.slope
                )
                # Add the Mann-Kendall test results to the dataframe
                df.loc[
                    (df["year"] >= xi.min()) & (df["year"] <= xi.max()),
                    [
                        "mk_trend",
                        "mk_ha_test",
                        "mk_pvalue",
                        "mk_slope",
                        "mk_intercept",
                    ],
                ] = [
                    mk_results.trend,
                    mk_results.h,
                    mk_results.p,
                    mk_results.slope,
                    mk_results.intercept,
                ]
        # Return values related to most recent year at disposal
        if last_value:
            df = df.sort_values(by="year", ascending=False)[0:1]
    else:
        df = pd.DataFrame(
            columns=[
                *df.columns,
                "slope",
                "intercept",
                "rsquared",
                "pvalue",
                "stderr_slope",
                "stderr_intercept",
                "year_range_lower",
                "year_range_upper",
                "mk_trend",
                "mk_ha_test",
                "mk_pvalue",
                "mk_slope",
                "mk_intercept",
            ]
        )
    return df


def segmented_regression(
    df,
    last_value=True,
    function="RSS",
    min_data_points=7,
    alpha=0.05,
    multi_process=False,
    groupby_column_list=[],
    value_column=None,
):
    """
    Calculate the linear regression statistics of the segmented regression
    based on the optimization of the objective function

    :param (DataFrame) df, dataframe containing the time series with respect to calculate the segmented regressions
    :param (boolean) last_value, if True returns only values related to the most recent year (default = True)
    :param (string) function, the objective to calculate could be "R2" or "RSS", i.e. coefficient of determination based or
        residual sum of squares based (default = "RSS")
    :param (int) min_data_points, minimum nr of data for each segment of the linear regression (default = 7)
    :param (float) alpha, significance level of statistical tests (default = 0.05)
    :param multi_process (Boolean), if True segmented regression is performed through multiple cores. Default is False
    :param groupby_column_list (List), columns to be grouped. Default is empty list
    :param value_column (string), column which refers to data. Default is None

    :return (DataFrame) df_segmented_regression, dataframe which contains new columns related to
    linear regression calculations

    As example, compute the segmented regression for soybean exports from Brazil as reporter to Europe and
    rest of the world as partners.
    Import dependencies

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> from biotrade.common.time_series import segmented_regression

    Select soybean trade products with Brazil as reporter

        >>> soybean_trade = faostat.db.select(table="crop_trade",
        >>>     reporter="Brazil", product_code=236)

    Aggregate partners as Europe (eu) and rest of the World (row)

        >>> soybean_trade_agg = agg_trade_eu_row(soybean_trade)

    Select export quantity

        >>> soybean_exp_agg = soybean_trade_agg[soybean_trade_agg["element"] == "export_quantity"]

    Calculate the segmented regression of values returning the most recent year, first using RSS and then R2 objective function, keeping the minimum number of points and the significance level as default

        >>> soybean_exp_rss = segmented_regression(soybean_exp_agg, last_value = True, function = "RSS")
        >>> soybean_exp_r2 = segmented_regression(soybean_exp_agg, last_value = True, function = "R2")

    """

    # Define value column name if not provided
    if value_column is None:
        if "value" in df.columns:
            # Value column of faostat db
            value_column = "value"
        elif "net_weight" in df.columns:
            # Value column is called "net_weight" in comtrade db
            value_column = "net_weight"
        else:
            raise ValueError("Provide column name containing values")
    # Define group columns if not provided
    if len(groupby_column_list) == 0:
        groupby_column_list = ["reporter", "product_code", "unit"]
        # Depending on the db tables, different columns can be used to group the dataframe
        add_columns = ["source", "partner", "element", "flow", "unit_code"]
        # Check if columns are present in the original dataframe
        for column in add_columns:
            if column in df.columns:
                # For comtrade db, do not consider unit and unit_code columns which are always nan
                if column == "unit_code":
                    groupby_column_list.remove("unit")
                    continue
                groupby_column_list.append(column)
    # Group by df according to columns previously defined
    groupby = df.groupby(
        groupby_column_list,
        as_index=False,
    )
    # Define the function and arguments to be applied
    func = partial(
        optimize_breakpoints,
        value_column=value_column,
        min_data_points=min_data_points,
        alpha=alpha,
        last_value=last_value,
        function=function,
    )
    # Perform multiprocessing
    if multi_process:
        # Define data groups
        groups = [groupby.get_group(group) for group in groupby.groups]
        with Pool() as pool:
            # Launch parallel jobs using several cores depending on the machine
            segmented_regression = pool.map(
                func,
                groups,
            )
            # Concat results of the multi processing
            df_segmented_regression = pd.concat(
                segmented_regression, ignore_index=True
            )
    else:
        # Use the pd.apply function
        df_segmented_regression = groupby.apply(func).reset_index(drop=True)
    # Transform year lower and upper columns into int type
    df_segmented_regression[
        ["year_range_lower", "year_range_upper"]
    ] = df_segmented_regression[
        ["year_range_lower", "year_range_upper"]
    ].astype(
        "int"
    )
    # Add information regarding the obj function used
    df_segmented_regression["obj_function"] = function
    # Add information regarding the significance level
    df_segmented_regression["significance_level"] = alpha
    # Final dataframe with the new column statistic columns
    return df_segmented_regression


def calculate_changes(
    df, column_list_calc, year_range, value_column, years, last_value
):
    """
    Function to calculate absolute and relative changes

    :param (DataFrame) df, dataframe containing 1 time series for the change calculations
    :param (list) column_list_calc, column name list on which performing calculations
    ::param (list) year_range -> [lower year (int), upper year (int)], if values are given the function computes the average value with respect to this range and does not consider the years argument
    :param (string) value_column, name of the column containing the data
    :param (int) years, temporal window over calculating the relative and absolute change
    :param (boolean) last_value, if True returns only values related to the most recent year

    :return (DataFrame) df, dataframe which contains new columns related to
    change calculations for the given time series
    """
    # Avoid unexpected behavior of pandas apply function on df
    df = df.copy()
    # Fill nan year values
    dict_values = {}
    merge_col_list = []
    for col in df.columns:
        if col in ["year", "period"]:
            dict_values.update(
                {
                    col: range(
                        sorted(df.year.unique())[0],
                        sorted(df.year.unique())[-1] + 1,
                    )
                }
            )
            merge_col_list.append(col)
        elif col in [
            "source",
            "reporter_code",
            "reporter",
            "partner_code",
            "partner",
            "product_code",
            "product",
            "element_code",
            "element",
            "flow",
            "unit",
            "unit_code",
        ]:
            dict_values.update({col: df[col].unique()[0]})
            merge_col_list.append(col)
    df_fill_nan = pd.DataFrame(dict_values)
    df = df.merge(df_fill_nan, on=merge_col_list, how="outer")
    # Sort data from the most recent year
    df = df.sort_values(by="year", ascending=False)
    # Add columns to dataframe
    for idx, column in enumerate(column_list_calc):
        # The last column of the list corresponds to the average value
        if column == "average_value":
            # The last column is the mean of the corresponding values of the year range
            if year_range:
                df[column] = df.loc[
                    (df["year"] >= year_range[0])
                    & (df["year"] <= year_range[1]),
                    value_column,
                ].mean()
            # The last column is the mean of the previous year values
            elif years > 0:
                df[column] = df[column_list_calc[:-1]].mean(
                    axis=1,
                )
            break
        # Shift value column up with respect to the number of years considered
        df[column] = df[value_column].shift(-(idx) - 1)
    # Finally calculate the relative and absolute change columns
    df["relative_change"] = (
        (df[value_column] - df["average_value"]) / df["average_value"] * 100
    )
    df["absolute_change"] = df[value_column] - df["average_value"]
    # 0/0 and float/0 treated as NaN
    df["relative_change"].replace([np.inf, -np.inf], np.nan, inplace=True)
    # Define 2 new columns with the lower and upper years of the average value
    if year_range:
        df["year_range_lower"] = year_range[0]
        df["year_range_upper"] = year_range[1]
    elif years > 0:
        df["year_range_lower"] = df["year"] - years
        df["year_range_upper"] = df["year"] - 1
    # Drop columns added for the calculation
    df = df.drop(column_list_calc[:-1], axis=1)
    # Return values related to most recent year at disposal
    if last_value:
        df = df[0:1]

    return df


def relative_absolute_change(
    df,
    years=5,
    last_value=True,
    multi_process=False,
    year_range=[],
    groupby_column_list=[],
    value_column=None,
):
    """
    Calculate the relative and absolute change of a quantity with respect to
    values assumed by that quantity in the n previous years or with respect to a year range given, for
    production and trade data.
    Relative change of a value is calculated as:
        (value of the reference year - mean of values of previous n years (or year range given)) / mean of values of previous n years (or year range given) * 100
    Absolute change of a value is calculated as:
        value of the reference year - mean of values of previous n years (or year range given)

    :param (DataFrame) df, dataframe containing the quantity with respect to calculate the relative and absolute change in time
    :param (int) years, temporal window over calculating the relative and absolute change (default = 5)
    :param (boolean) last_value, if True returns only values related to the most recent year (default = True)
    :param (list) year_range -> [lower year (int), upper year (int)], if given the function computes the average value with respect to this range and does not consider the years argument (default = empty)
    :param multi_process (Boolean), if True changes are performed through multiple cores. Default is False
    :param groupby_column_list (List), columns to be grouped. Default is empty list
    :param value_column (string), column which refers to data. Default is None

    :return (DataFrame) df_change, dataframe which contains new columns
        "average_value" mean of values of the year range
        "relative_change" value calculated
        "absolute_change" value calculated
        "year_range_lower" lower year with respect to compute the average value
        "year_range_upper" upper year with respect to compute the average value

    As example, compute the relative and absolute change for soy trade products from Brazil as reporter to Europe and
    rest of the world as partners.
    Import dependencies

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> from biotrade.common.time_series import relative_absolute_change

    Select soy trade products with Brazil as reporter

        >>> soy_trade = faostat.db.select(table="crop_trade",
        >>>     reporter="Brazil", product="soy")

    Aggregate partners as Europe (eu) and rest of the World (row)

        >>> soy_trade_agg = agg_trade_eu_row(soy_trade)

    Calculate the relative and absolute change of values in time for the most recent year, considering the last 5 years

        >>> soy_trade_change1 = relative_absolute_change(soy_trade_agg, years= 5, last_value = True)

    Calculate the relative and absolute change of values in time with respect to the average value between 2000 and 2010

        >>> soy_trade_change2 = relative_absolute_change(soy_trade_agg, year_range = [2000, 2010], last_value = False)

    """
    # Define value column name if not provided
    if value_column is None:
        if "value" in df.columns:
            # Value column of faostat db
            value_column = "value"
        elif "net_weight" in df.columns:
            # Value column is called "net_weight" in comtrade db
            value_column = "net_weight"
        else:
            raise ValueError("Provide column name containing values")
    # Define group columns if not provided
    if len(groupby_column_list) == 0:
        groupby_column_list = ["reporter", "product_code", "unit"]
        # Depending on the db tables, different columns can be used to group the dataframe
        add_columns = [
            "source",
            "partner",
            "element",
            "flow",
            "unit_code",
        ]
        # Check if columns are present in the original dataframe
        for column in add_columns:
            if column in df.columns:
                # For comtrade db, do not consider unit and unit_code columns which are always nan
                if column == "unit_code":
                    groupby_column_list.remove("unit")
                    continue
                groupby_column_list.append(column)
    # Define groups with respect to calculate the relative and absolute change of values in time
    groupby = df.groupby(groupby_column_list, as_index=False)
    # Allocate df reshaped to return
    df_change = pd.DataFrame()
    # Column list to perform calculations
    column_list_calc = []
    # If a year range is given, consider it to calculate the average value
    if year_range:
        years = 0
    # If a year range is not given, consider the n years before
    elif years > 0:
        # Add columns with respect to the number of years to calculate the relative and absolute change
        for year in range(1, years + 1):
            column_list_calc.append(f"value_{year}_year_before")
    # Colum to perform the average value
    column_list_calc.append("average_value")
    # Define the function and arguments to be applied
    func = partial(
        calculate_changes,
        column_list_calc=column_list_calc,
        year_range=year_range,
        value_column=value_column,
        years=years,
        last_value=last_value,
    )
    # Perform multiprocessing
    if multi_process:
        # Define data groups
        groups = [groupby.get_group(group) for group in groupby.groups]
        with Pool() as pool:
            # Launch parallel jobs using several cores depending on the machine
            relative_absolute = pool.map(
                func,
                groups,
            )
            # Concat results of the multi processing
            df_change = pd.concat(relative_absolute, ignore_index=True)
    else:
        # Use the pd.apply function
        df_change = groupby.apply(func).reset_index(drop=True)
    # Final dataframe with the new columns
    return df_change


def merge_analysis(df_change, df_segmented_regression):
    """
    Function to merge dataframes related to change and segmented regression analysis, in order to compare results.

    :param (DataFrame) df_change, df obtained from relative_absolute_change function
    :param (DataFrame) df_change, df obtained from segmented_regression function
    :return (DataFrame) df, which is a merge of the input dataframes

    For example select soybean trade product with Brazil as reporter, aggregate partners as Europe (eu) and rest of the World (row)
    and compute the change and segmented analysis. Finally compare results with the merge function

    >>> from biotrade.faostat import faostat
    >>> from biotrade.faostat.aggregate import agg_trade_eu_row
    >>> from biotrade.common.time_series import (relative_absolute_change, segmented_regression, merge_analysis)
    >>> soybean_trade = faostat.db.select( table="crop_trade", reporter="Brazil", product_code=236)
    >>> soybean_trade_agg = agg_trade_eu_row(soybean_trade)
    >>> soybeans_exp_change = relative_absolute_change(soybean_trade_agg, last_value=True)
    >>> soybean_exp_regression = segmented_regression(soybean_trade_agg, last_value=True, function="R2", alpha=0.05, min_data_points=7)
    >>> df_merge = merge_analysis(df_change=soybeans_exp_change, df_segmented_regression=soybean_exp_regression)

    """
    # Columns in common for the two dataframes
    join_columns = list(
        set(df_change.columns) & set(df_segmented_regression.columns)
    )
    # Columns not to be considered for the join
    remove_columns = ["year_range_lower", "year_range_upper"]
    # Final join columns
    join_columns = list(set(join_columns) - set(remove_columns))
    # Merge the two dataframes
    df = df_change.merge(
        df_segmented_regression,
        on=join_columns,
        how="outer",
        suffixes=("_change", "_regression"),
    )
    # Columns to be retained
    df = df[
        [
            *join_columns,
            "relative_change",
            "absolute_change",
            "year_range_lower_change",
            "year_range_upper_change",
            "mk_slope",
            "mk_ha_test",
            "year_range_lower_regression",
            "year_range_upper_regression",
        ]
    ]

    return df
