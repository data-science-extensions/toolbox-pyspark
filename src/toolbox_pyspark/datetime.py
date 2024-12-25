# ============================================================================ #
#                                                                              #
#     Title   : Datetime                                                       #
#     Purpose : Fixing column names that contain datetime data, adding         #
#               conversions to local datetimes, and for splitting a column in  #
#               to their date and time components.                             #
#                                                                              #
# ============================================================================ #


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Overview                                                              ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Description                                                              ####
# ---------------------------------------------------------------------------- #


"""
!!! note "Summary"
    The `datetime` module is used for fixing column names that contain datetime data, adding conversions to local datetimes, and for splitting a column in to their date and time components.
"""


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from typing import Optional, Union

# ## Python Third Party Imports ----
from pyspark.sql import Column, DataFrame as psDataFrame, functions as F
from toolbox_python.checkers import is_all_in, is_in, is_type
from toolbox_python.collection_types import str_collection, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import assert_column_exists, assert_columns_exists
from toolbox_pyspark.columns import get_columns


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "rename_datetime_column",
    "rename_datetime_columns",
    "add_local_datetime_column",
    "add_local_datetime_columns",
    "split_datetime_column",
    "split_datetime_columns",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Renaming                                                                 ####
# ---------------------------------------------------------------------------- #


@typechecked
def rename_datetime_column(
    dataframe: psDataFrame,
    column: str,
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given column in a Data Frame, if there is not another column existing that has `TIME` appended to the end, then re-name the column to append `TIME` to it.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        column (str):
            The column to check.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If the `#!py column` does not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated Data Frame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import rename_datetime_column
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c_date": pd.date_range(start="2022-01-01", periods=4, freq="h"),
        ...             "d_date": pd.date_range(start="2022-02-01", periods=4, freq="h"),
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |              c_date |              d_date |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Update column"}
        >>> rename_datetime_column(df, "c_date").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |          c_dateTIME |              d_date |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully renamed column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Missing column"}
        >>> rename_datetime_column(df, "fff")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "fff" does not exist in "dataframe".
        Try one of: ["a", "b", "c_date", "d_date"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

    ??? tip "See Also"
        - [`assert_column_exists()`][toolbox_pyspark.checks.assert_column_exists]
        - [`rename_datetime_columns()`][toolbox_pyspark.datetime.rename_datetime_columns]
    """
    assert_column_exists(dataframe=dataframe, column=column, match_case=True)
    if f"{column}TIME" not in dataframe.columns:
        return dataframe.withColumnRenamed(column, f"{column}TIME")
    else:
        return dataframe


@typechecked
def rename_datetime_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str_collection, str]] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Fix the column names for the date-time columns.

    ???+ abstract "Details"
        This is necessary because in NGW, there are some columns which have `datetime` data types, but which have the name only containing `date`.
        So, this function will fix that.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        columns (Optional[Union[str_collection, str]], None):
            An optional list of columns to update.
            If this is not provided, or is the value `#!py None` or `#!py "all"`, then the function will automatically determine which columns to update based on the following logic:

            1. Loop through each column on `dataframe` to fetch the name and dtype using the method: `dataframe.dtypes`.
                1. If the column name ends with `#!py "date"`
                2. **AND** the column type is `#!py "timestamp"`
                3. **AND** there is **NOT** already a column existing in the `dataframe.columns` with the name: `f"{column}TIME"`
                4. **THEN** rename the column to have the name: `f"{column}TIME"`
            2. Next column.

            Default: `None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import rename_datetime_column
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c_date": pd.date_range(start="2022-01-01", periods=4, freq="h"),
        ...             "d_date": pd.date_range(start="2022-02-01", periods=4, freq="h"),
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |              c_date |              d_date |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: One column"}
        >>> rename_datetime_column(df, ["c_date"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |          c_dateTIME |              d_date |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully renamed column."
        </div>

        ```{.py .python linenums="1" title="Example 2: One column `str`"}
        >>> rename_datetime_column(df, "c_date").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |          c_dateTIME |              d_date |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully renamed column."
        </div>

        ```{.py .python linenums="1" title="Example 3: All columns"}
        >>> rename_datetime_column(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |          c_dateTIME |          d_dateTIME |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully renamed columns."
        </div>

        ```{.py .python linenums="1" title="Example 4: All columns using 'all'"}
        >>> rename_datetime_column(df, "all").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+
        | a | b |          c_dateTIME |          d_dateTIME |
        +---+---+---------------------+---------------------+
        | 0 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 1 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 2 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 3 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully renamed columns."
        </div>

        ```{.py .python linenums="1" title="Example 5: Missing column"}
        >>> rename_datetime_columns(df, ["fff", "ggg"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Error: Columns ["fff", "ggg"] do not exist in "dataframe".
        Try one of: ["a", "b", "c_dateTIME", "d_dateTIME"].
        ```
        !!! failure "Conclusion: Columns do not exist."
        </div>

    ??? tip "See Also"
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`rename_datetime_column()`][toolbox_pyspark.datetime.rename_datetime_column]
    """
    if columns is None or columns == "all":
        datetime_cols: str_list = [
            col
            for col in get_columns(dataframe, "all_datetime")
            if col.lower().endswith("date")
        ]
        columns = [
            col
            for col in datetime_cols
            if col.lower().endswith("date") and f"{col}TIME" not in dataframe.columns
        ]
    elif is_type(columns, str):
        columns = [columns]
    assert_columns_exists(dataframe, columns, True)
    for column in columns:
        dataframe = rename_datetime_column(dataframe, column)
    return dataframe


# ---------------------------------------------------------------------------- #
#  Add Locals                                                               ####
# ---------------------------------------------------------------------------- #


@typechecked
def add_local_datetime_column(
    dataframe: psDataFrame,
    column: str,
    from_timezone: Optional[str] = None,
    column_with_target_timezone: str = "timezone_location".upper(),
) -> psDataFrame:
    """
    !!! note "Summary"
        For the given `column`, add a new column with the suffix `_LOCAL` which is a conversion of the datetime values from `column` to the desired timezone.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be fixed
        column (str):
            The name of the column to do the conversion for. Must exist in `#!py dataframe.columns`, and must be type `typestamp`.
        from_timezone (str, optional):
            The timezone which will be converted from. Must be a valid TimeZoneID, for more info, see: [TimeZoneID](https://docs.oracle.com/middleware/12211/wcs/tag-ref/MISC/TimeZones.html).<br>
            If not given, will default the `from_timezone` to be UTC.<br>
            Default: `#!py None`.
        column_with_target_timezone (str, optional):
            The column containing the target timezone value. By default will be the column `#!py "timezone_location"`.<br>
            Defaults to `#!py "timezone_location".upper()`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If `#!py "column"` or `column_with_target_timezone` does not exist within `#!py dataframe.columns`.
        ValueError:
            If the `from_timezone` or `column_with_target_timezone` is not a valid timezone.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import add_local_datetime_column
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        ...             "d": pd.date_range(start="2022-02-01", periods=4, freq="D"),
        ...             "e": pd.date_range(start="2022-03-01", periods=4, freq="D"),
        ...             "target": ["Asia/Singapore"] * 4,
        ...             "TIMEZONE_LOCATION": ["Australia/Perth"] * 4,
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        | a | b |                   c |                   d |                   e |         target | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Converting from UTC time"}
        >>> add_local_datetime_column(df, "c").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+
        | a | b |                   c |                   d |                   e |         target | TIMEZONE_LOCATION |             c_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-01 08:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-02 08:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-03 08:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-04 08:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted from UTC."
        </div>

        ```{.py .python linenums="1" title="Example 2: Converting from specific timezone, with custom column containing target timezone"}
        >>> add_local_datetime_column(
        ...     dataframe=df,
        ...     column="c",
        ...     from_timezone="Australia/Sydney",
        ...     column_with_target_timezone="target",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | a | b |                   c |                   d |                   e |         target | TIMEZONE_LOCATION |               c_UTC |             c_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2021-12-31 13:00:00 | 2021-12-31 21:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-01 13:00:00 | 2022-01-01 21:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-02 13:00:00 | 2022-01-02 21:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-03 13:00:00 | 2022-01-03 21:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted timezone."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid column name"}
        >>> add_local_datetime_column(df, "invalid_column")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "invalid_column" does not exist in "dataframe".
        Try one of: ["a", "b", "c", "d", "e", "target", "TIMEZONE_LOCATION"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid timezone"}
        >>> add_local_datetime_column(df, "c", from_timezone="Invalid/Timezone")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ValueError: The timezone "Invalid/Timezone" is not a valid timezone.
        ```
        !!! failure "Conclusion: Invalid timezone."
        </div>

    ??? info "Notes"
        - If `#!py from_timezone is None`, then it is assumed that the datetime data in `column` is _already_ in UTC timezone.<br>
        - If `#!py from_timezone is not None`, then a new column will be added with the syntax `#!py {column}_UTC`, then another column added with `#!py {column}_LOCAL`. This is necessary because PySpark cannot convert immediately from one timezone to another; it must first require a conversion from the `from_timezone` value _to_ UTC, then a second conversion _from_ UTC to whichever timezone is defined in the column `column_with_target_timezone`.<br>
        - The reason why this function uses multiple [`.withColumn()`][withColumn] methods, instead of a single [`.withColumns()`][withColumns] expression is because to add the `#!py {column}_LOCAL` column, it is first necessary for the `#!py {column}_UTC` column to exist on the `dataframe`. Therefore, we need to call [`.withColumn()`][withColumn] first to add `#!py {column}_UTC`, then we need to call [`.withColumn()`][withColumn] a second time to add `#!py {column}_LOCAL`.
        [withColumn]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
        [withColumns]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html

    ??? tip "See Also"
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`add_local_datetime_columns()`][toolbox_pyspark.datetime.add_local_datetime_columns]
    """
    assert_columns_exists(dataframe=dataframe, columns=[column, column_with_target_timezone])
    require_utc: bool = f"{column}_UTC" not in dataframe.columns
    require_local: bool = f"{column}_LOCAL" not in dataframe.columns
    if from_timezone is not None:
        if require_utc:
            dataframe = dataframe.withColumn(
                f"{column}_UTC",
                F.to_utc_timestamp(
                    F.col(column).cast("timestamp"),
                    from_timezone.title(),
                ),
            )
        if require_local:
            dataframe = dataframe.withColumn(
                f"{column}_LOCAL",
                F.from_utc_timestamp(
                    F.col(f"{column}_UTC").cast("timestamp"),
                    F.col(column_with_target_timezone),
                ),
            )
    else:
        if require_local:
            dataframe = dataframe.withColumn(
                f"{column}_LOCAL",
                F.from_utc_timestamp(
                    F.col(column).cast("timestamp"),
                    F.col(column_with_target_timezone),
                ),
            )
    return dataframe


@typechecked
def add_local_datetime_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
    from_timezone: Optional[str] = None,
    column_with_target_timezone: str = "timezone_location".upper(),
) -> psDataFrame:
    """
    !!! note "Summary"
        For each of the `data` or `datetime` columns in `dataframe`, add a new column which is converting it to the timezone of the local datetime.

    ???+ abstract "Details"
        Under the hood, this function will call [`add_local_datetime_column()`][toolbox_pyspark.datetime.add_local_datetime_column] for each `column` in `columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        columns (Optional[Union[str, str_collection]], optional):
            The columns to check. If not provided, it will use all of the columns which contains the text `date`.<br>
            Defaults to `#!py None`.
        from_timezone (Optional[str], optional):
            The timezone which will be converted from. If not given, will default the from timezone to be UTC.<br>
            Defaults to `#!py None`.
        column_with_target_timezone (str, optional):
            The column containing the target timezone value. By default will be the column `#!py "timezone_location"`.<br>
            Defaults to `#!py "timezone_location".upper()`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.
        ValueError:
            If the `from_timezone` or `column_with_target_timezone` is not a valid timezone.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import add_local_datetime_columns
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": pd.date_range(start="2022-01-01", periods=4, freq="D"),
        ...             "d_datetime": pd.date_range(start="2022-02-01", periods=4, freq="D"),
        ...             "e_datetime": pd.date_range(start="2022-03-01", periods=4, freq="D"),
        ...             "target": ["Asia/Singapore"] * 4,
        ...             "TIMEZONE_LOCATION": ["Australia/Perth"] * 4,
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default config"}
        >>> add_local_datetime_columns(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |    d_datetime_LOCAL |    e_datetime_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-01 08:00:00 | 2022-03-01 08:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-02 08:00:00 | 2022-03-02 08:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-03 08:00:00 | 2022-03-03 08:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-04 08:00:00 | 2022-03-04 08:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted columns to local timezone."
        </div>

        ```{.py .python linenums="1" title="Example 2: Semi-custom config"}
        >>> add_local_datetime_columns(df, ["c", "d_datetime"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |             c_LOCAL |    d_datetime_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-01 08:00:00 | 2022-02-01 08:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-02 08:00:00 | 2022-02-02 08:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-03 08:00:00 | 2022-02-03 08:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-04 08:00:00 | 2022-02-04 08:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted columns to local timezone."
        </div>

        ```{.py .python linenums="1" title="Example 3: Full-custom config"}
        >>> add_local_datetime_columns(
        ...     dataframe=df,
        ...     columns=["c", "d_datetime"],
        ...     from_timezone="Australia/Sydney",
        ...     column_with_target_timezone="target",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |               c_UTC |             c_LOCAL |      d_datetime_UTC |    d_datetime_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2021-12-31 13:00:00 | 2021-12-31 21:00:00 | 2022-01-31 13:00:00 | 2022-02-01 08:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-01 13:00:00 | 2022-01-01 21:00:00 | 2022-02-01 13:00:00 | 2022-02-02 08:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-02 13:00:00 | 2022-01-02 21:00:00 | 2022-02-02 13:00:00 | 2022-02-03 08:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-03 13:00:00 | 2022-01-03 21:00:00 | 2022-02-03 13:00:00 | 2022-02-04 08:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted columns to local time zone, from other custom time zone."
        </div>

        ```{.py .python linenums="1" title="Example 4: Single column"}
        >>> add_local_datetime_columns(
        ...     dataframe=df,
        ...     columns="c",
        ...     from_timezone="Australia/Sydney",
        ...     column_with_target_timezone="target",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |               c_UTC |             c_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2021-12-31 13:00:00 | 2021-12-31 21:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-01 13:00:00 | 2022-01-01 21:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-02 13:00:00 | 2022-01-02 21:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-03 13:00:00 | 2022-01-03 21:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted single column from other time zone to local time zone."
        </div>

        ```{.py .python linenums="1" title="Example 5: All columns"}
        >>> add_local_datetime_columns(
        ...     dataframe=df,
        ...     columns="all",
        ...     from_timezone="Australia/Sydney",
        ...     column_with_target_timezone="target",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        | a | b |                   c |          d_datetime |          e_datetime |         target | TIMEZONE_LOCATION |      d_datetime_UTC |    d_datetime_LOCAL |      e_datetime_UTC |    e_datetime_LOCAL |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-01-31 13:00:00 | 2022-02-01 08:00:00 | 2022-02-28 13:00:00 | 2022-03-01 08:00:00 |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-01 13:00:00 | 2022-02-02 08:00:00 | 2022-03-01 13:00:00 | 2022-03-02 08:00:00 |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-02 13:00:00 | 2022-02-03 08:00:00 | 2022-03-02 13:00:00 | 2022-03-03 08:00:00 |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Asia/Singapore |   Australia/Perth | 2022-02-03 13:00:00 | 2022-02-04 08:00:00 | 2022-03-03 13:00:00 | 2022-03-04 08:00:00 |
        +---+---+---------------------+---------------------+---------------------+----------------+-------------------+---------------------+---------------------+---------------------+---------------------+
        ```
        !!! success "Conclusion: Successfully converted all date time columns from other time zone to local time zone."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid column name"}
        >>> add_local_datetime_columns(df, "invalid_column")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "invalid_column" does not exist in "dataframe".
        Try one of: ["a", "b", "c", "d_datetime", "e_datetime", "target", "TIMEZONE_LOCATION"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 7: Invalid timezone"}
        >>> add_local_datetime_columns(df, "c", from_timezone="Invalid/Timezone")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ValueError: The timezone "Invalid/Timezone" is not a valid timezone.
        ```
        !!! failure "Conclusion: Invalid timezone."
        </div>

    ??? tip "See Also"
        - [`add_local_datetime_column()`][toolbox_pyspark.datetime.add_local_datetime_column]
    """
    if columns is None or columns in ["all"]:
        columns = [col for col in dataframe.columns if col.lower().endswith("datetime")]
    elif is_type(columns, str):
        columns = [columns]
    assert_columns_exists(dataframe, list(columns) + [column_with_target_timezone])
    for column in columns:
        dataframe = add_local_datetime_column(
            dataframe=dataframe,
            column=column,
            from_timezone=from_timezone,
            column_with_target_timezone=column_with_target_timezone,
        )
    return dataframe


# ---------------------------------------------------------------------------- #
#  Splitting                                                                ####
# ---------------------------------------------------------------------------- #


@typechecked
def split_datetime_column(
    dataframe: psDataFrame,
    column: str,
) -> psDataFrame:
    """
    !!! note "Summary"
        Take the column `column`, which should be a `timestamp` type, and split it in to it's respective `date` and `time` components.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        column (str):
            The column to split.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If the `#!py column` does not exist within `#!py dataframe.columns`.
        TypeError:
            If the `column` is not type `timestamp` or `datetime`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import split_datetime_column
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c_datetime": pd.date_range(start="2022-01-01", periods=4, freq="h"),
        ...             "d_datetime": pd.date_range(start="2022-02-01", periods=4, freq="h"),
        ...             "e_datetime": pd.date_range(start="2022-03-01", periods=4, freq="h"),
        ...             "TIMEZONE_LOCATION": ["Australia/Perth"] * 4,
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default config"}
        >>> split_datetime_column(df, "c_datetime").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |     C_DATE |   C_TIME |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth | 2022-01-01 | 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth | 2022-01-01 | 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth | 2022-01-01 | 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth | 2022-01-01 | 03:00:00 |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        ```
        !!! success "Conclusion: Successfully split the column in to it's Date and Time constituents."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid column name"}
        >>> split_datetime_column(df, "invalid_column")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "invalid_column" does not exist in "dataframe".
        Try one of: ["a", "b", "c_datetime", "d_datetime", "e_datetime", "TIMEZONE_LOCATION"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid column name"}
        >>> split_datetime_column(df, "b")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        TypeError: Column must be type 'timestamp' or 'datetime'.
        Current type: [('b', 'string')]
        ```
        !!! failure "Conclusion: Column is not the correct type for splitting."
        </div>

    ??? tip "See Also"
        - [`split_datetime_columns()`][toolbox_pyspark.datetime.split_datetime_columns]
    """
    assert_column_exists(dataframe, column)
    datetime_cols: str_list = get_columns(dataframe, "all_datetime")
    if not is_in(column, datetime_cols):
        raise TypeError(
            "Column must be type 'timestamp' or 'datetime'.\n"
            f"Current type: {[(col,typ) for col,typ in dataframe.dtypes if col == column]}"
        )
    col_date_name: str = column.upper().replace("DATETIME", "DATE")
    col_time_name: str = column.upper().replace("DATETIME", "TIME")
    col_date_value: Column = F.date_format(column, "yyyy-MM-dd").cast("string").cast("date")
    col_time_value: Column = F.date_format(column, "HH:mm:ss").cast("string")
    return dataframe.withColumns(
        {
            col_date_name: col_date_value,
            col_time_name: col_time_value,
        }
    )


@typechecked
def split_datetime_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        For all the columns listed in `columns`, split them each in to their respective `date` and `time` components.

    ???+ abstract "Details"
        The reason why this function is structured this way, and not re-calling [`split_datetime_column()`][toolbox_pyspark.datetime.split_datetime_column] in each iteration of `columns` is due to `#!py pyspark` RDD complexity. More specifically, if it _were_ to call [`split_datetime_column()`][toolbox_pyspark.datetime.split_datetime_column] each time, the RDD would get incredibly and unnecessarily complicated. However, by doing it this way, using the [`.withColumns()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html) method, it will project the SQL expression **once** down to the underlying dataframe; not multiple times. Therefore, in this way, the underlying SQL execution plan is now much less complicated; albeit that the coding DRY principle is not strictly being followed here.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        columns (Optional[Union[str, str_collection]], optional):
            The list of columns to update. If not given, it will generate the list of columns from the `#!py dataframe.columns` which contain the text `datetime`.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.
        TypeError:
            If any of the columns in `columns` are not type `timestamp` or `datetime`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.datetime import split_datetime_columns
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c_datetime": pd.date_range(start="2022-01-01", periods=4, freq="h"),
        ...             "d_datetime": pd.date_range(start="2022-02-01", periods=4, freq="h"),
        ...             "e_datetime": pd.date_range(start="2022-03-01", periods=4, freq="h"),
        ...             "TIMEZONE_LOCATION": ["Australia/Perth"] * 4,
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default config"}
        >>> split_datetime_columns(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |     C_DATE |   C_TIME |     D_DATE |   D_TIME |     E_DATE |   E_TIME |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth | 2022-01-01 | 00:00:00 | 2022-02-01 | 00:00:00 | 2022-03-01 | 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth | 2022-01-01 | 01:00:00 | 2022-02-01 | 01:00:00 | 2022-03-01 | 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth | 2022-01-01 | 02:00:00 | 2022-02-01 | 02:00:00 | 2022-03-01 | 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth | 2022-01-01 | 03:00:00 | 2022-02-01 | 03:00:00 | 2022-03-01 | 03:00:00 |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        ```
        !!! success "Conclusion: Successfully split all DateTime columns in to their Date and Time constituents."
        </div>

        ```{.py .python linenums="1" title="Example 2: Custom config"}
        >>> split_datetime_columns(df, ["c_datetime", "d_datetime"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |     C_DATE |   C_TIME |     D_DATE |   D_TIME |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth | 2022-01-01 | 00:00:00 | 2022-02-01 | 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth | 2022-01-01 | 01:00:00 | 2022-02-01 | 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth | 2022-01-01 | 02:00:00 | 2022-02-01 | 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth | 2022-01-01 | 03:00:00 | 2022-02-01 | 03:00:00 |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+
        ```
        !!! success "Conclusion: Successfully split two columns into their Date and Time constituents."
        </div>

        ```{.py .python linenums="1" title="Example 3: All columns"}
        >>> split_datetime_columns(df, "all").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |     C_DATE |   C_TIME |     D_DATE |   D_TIME |     E_DATE |   E_TIME |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth | 2022-01-01 | 00:00:00 | 2022-02-01 | 00:00:00 | 2022-03-01 | 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth | 2022-01-01 | 01:00:00 | 2022-02-01 | 01:00:00 | 2022-03-01 | 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth | 2022-01-01 | 02:00:00 | 2022-02-01 | 02:00:00 | 2022-03-01 | 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth | 2022-01-01 | 03:00:00 | 2022-02-01 | 03:00:00 | 2022-03-01 | 03:00:00 |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+------------+----------+------------+----------+
        ```
        !!! success "Conclusion: Successfully split all DateTime columns in to their Date and Time constituents."
        </div>

        ```{.py .python linenums="1" title="Example 4: Single column"}
        >>> split_datetime_columns(df, "c_datetime").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |     C_DATE |   C_TIME |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth | 2022-01-01 | 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth | 2022-01-01 | 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth | 2022-01-01 | 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth | 2022-01-01 | 03:00:00 |
        +---+---+---------------------+---------------------+---------------------+-------------------+------------+----------+
        ```
        !!! success "Conclusion: Successfully split a single column in to it's Date and Time constituents."
        </div>

        ```{.py .python linenums="1" title="Example 5: Invalid column name"}
        >>> split_datetime_columns(df, "invalid_column")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "invalid_column" does not exist in "dataframe".
        Try one of: ["a", "b", "c_datetime", "d_datetime", "e_datetime", "TIMEZONE_LOCATION"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid column type"}
        >>> split_datetime_columns(df, "b")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        TypeError: Column must be type 'timestamp' or 'datetime'.
        Current type: [('b', 'string')]
        ```
        !!! failure "Conclusion: Column is not the correct type for splitting."
        </div>

    ??? tip "See Also"
        - [`split_datetime_column()`][toolbox_pyspark.datetime.split_datetime_column]
    """
    if columns is None or columns in ["all"]:
        columns = [col for col in dataframe.columns if "datetime" in col.lower()]
    elif is_type(columns, str):
        columns = [columns]
    assert_columns_exists(dataframe, columns)
    datetime_cols: str_list = get_columns(dataframe, "all_datetime")
    if not is_all_in(columns, datetime_cols):
        raise TypeError(
            "Columns to split must be type 'timestamp' or 'datetime'.\n"
            f"Current types: {[(col,typ) for col,typ in dataframe.dtypes if col in columns]}"
        )
    cols_exprs: dict[str, Column] = {}
    for column in columns:
        col_date_name: str = column.upper().replace("DATETIME", "DATE")
        col_time_name: str = column.upper().replace("DATETIME", "TIME")
        col_date_value: Column = (
            F.date_format(column, "yyyy-MM-dd").cast("string").cast("date")
        )
        col_time_value: Column = F.date_format(column, "HH:mm:ss").cast("string")
        cols_exprs[col_date_name] = col_date_value
        cols_exprs[col_time_name] = col_time_value
    return dataframe.withColumns(cols_exprs)
