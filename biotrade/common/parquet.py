"""Functions to read parquet files with heterogenous schemas

For example between 1999 and 2000 the Comtrade bilateral trade columns altqty
and grosswgt change from null to double. The following functions display column
types in parquet files and find the type if there is only null and a data type.

"""

import pathlib
from typing import Union
import pyarrow.dataset
import pyarrow.parquet
import pandas


def get_parquet_column_types(directory: Union[str, pathlib.Path]) -> pandas.DataFrame:
    """
    List column types for all parquet files in all partitions of a dataset directory.

    Parameters
    ----------
    product_directory : str or Path
        Root directory of the partitioned parquet dataset.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: file_path, and all columns in the data

    Examples
    --------
    >>> import pathlib
    >>> product_directory = pathlib.Path.home() / "rp/forobs/biotrade_data/comtrade/products/download_date=20250906/product_code=440791"
    >>> df = get_parquet_column_types(product_directory)
    >>> # If there is more than one data type print the column name
    >>> for col in df.columns[1:]:
    ...     unique_values = df[col].unique()
    ...     if len(unique_values) > 1:
    ...         print(col, "has more than one value:", unique_values)
    """
    directory = pathlib.Path(directory)
    df_all = pandas.DataFrame()
    for parquet_file in directory.glob("**/*.parquet"):
        try:
            pf = pyarrow.parquet.ParquetFile(parquet_file)
            # Convert to an arrow schema to get the types attributes
            schema = pf.schema.to_arrow_schema()
            df = pandas.DataFrame(
                {
                    "file_name": str(parquet_file),
                    "column_name": schema.names,
                    "column_type": schema.types,
                }
            )
        except Exception as e:
            df = pandas.DataFrame(
                {
                    "file_name": str(parquet_file),
                    "column_name": "<ERROR>",
                    "column_type": str(e),
                }
            )
        df_all = pandas.concat([df_all, df])
    # Reshape to wide format
    df_all_wide = df_all.pivot(
        columns="column_name", index="file_name", values="column_type"
    )
    return df_all_wide.reset_index()


def harmonize_parquet_schema(directory):
    """Remove type differences in parquet schema

    In case the column type changes through time, there will be different
    types and pyarrow cannot read mixed data types. This function returns the
    type value that is not null.

    Get a data frame with column types in all files. Then when there are 2
    different column types, only keep the one that is not null and re-create a
    parquet schema definition based on that type.

    Examples
    --------
    >>> import pathlib
    >>> product_directory = pathlib.Path.home() / "rp/forobs/biotrade_data/comtrade/products/download_date=20250906/product_code=440791"
    >>> schema = harmonize_parquet_schema(product_directory)
    """
    dataset = pyarrow.dataset.dataset(directory, format="parquet")
    original_schema = dataset.schema
    # Get column types for all schema from all files
    df = get_parquet_column_types(directory)
    fields = []
    for field in original_schema:
        unique_values = df[field.name].unique()
        n_types = len(unique_values)
        non_null_type = unique_values[unique_values != pyarrow.null()]
        is_float64 = unique_values[unique_values != pyarrow.null()] == pyarrow.float64()
        if n_types == 1:
            fields.append(field)
        elif (n_types == 2) and is_float64:
            fields.append(
                pyarrow.field(
                    field.name,
                    pyarrow.float64(),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        else:
            msg = f"Column '{field.name}' has mixed data types: {unique_values} "
            msg += "and cannot be handled by this function.\n"
            msg += "For more details, check:\n"
            msg += f"get_parquet_column_types('{directory}')\n"
            raise ValueError(msg)
    new_schema = pyarrow.schema(fields)
    return new_schema
