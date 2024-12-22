# ============================================================================ #
#                                                                              #
#     Title   : Checks                                                         #
#     Purpose : Check and validate various attributed about a given `pyspark`  #
#               `dataframe`.                                                   #
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
    The `checks` module is used to check and validate various attributed about a given `pyspark` dataframe.
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
from dataclasses import dataclass, fields
from typing import Union
from warnings import warn

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.constants import VALID_PYSPARK_TYPE_NAMES
from toolbox_pyspark.io import read_from_path
from toolbox_pyspark.utils.warnings import AttributeWarning


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "column_exists",
    "columns_exists",
    "assert_column_exists",
    "assert_columns_exists",
    "warn_column_missing",
    "warn_columns_missing",
    "is_vaid_spark_type",
    "table_exists",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Column Existence                                                         ####
# ---------------------------------------------------------------------------- #


@dataclass
class ColumnExistsResult:
    result: bool
    missing_cols: str_list

    def __iter__(self):
        for field in fields(self):
            yield getattr(self, field.name)


@typechecked
def _columns_exists(
    dataframe: psDataFrame,
    columns: str_collection,
    match_case: bool = False,
) -> ColumnExistsResult:
    cols: str_collection = columns if match_case else [col.upper() for col in columns]
    df_cols: str_list = (
        dataframe.columns
        if match_case
        else [df_col.upper() for df_col in dataframe.columns]
    )
    missing_cols: str_list = [col for col in cols if col not in df_cols]
    return ColumnExistsResult(len(missing_cols) == 0, missing_cols)


@typechecked
def column_exists(
    dataframe: psDataFrame,
    column: str,
    match_case: bool = False,
) -> bool:
    """
    !!! note "Summary"
        Check whether a given `#!py column` exists as a valid column within `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check.
        column (str):
            The column to check.
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            If `#!py False`, will default to: `#!py column.upper()`.<br>
            Default: `#!py False`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (bool):
            `#!py True` if exists or `#!py False` otherwise.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example1: Column Exists"}
        >>> result = column_exists(df, "a")
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Column exists."
        </div>

        ```{.py .python linenums="1" title="Example 2: Column Missing"}
        >>> result = column_exists(df, "c")
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        False
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>
    """
    return _columns_exists(dataframe, [column], match_case).result


@typechecked
def columns_exists(
    dataframe: psDataFrame,
    columns: str_collection,
    match_case: bool = False,
) -> bool:
    """
    !!! note "Summary"
        Check whether all of the values in `#!py columns` exist in `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check
        columns (Union[str_list, str_tuple, str_set]):
            The columns to check
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            If `#!py False`, will default to: `#!py [col.upper() for col in columns]`.<br>
            Default: `#!py False`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (bool):
            `#!py True` if all columns exist or `#!py False` otherwise.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example 1: Columns exist"}
        >>> columns_exists(df, ["a", "b"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        True
        ```
        !!! success "Conclusion: All columns exist."
        </div>

        ```{.py .python linenums="1" title="Example 2: One column missing"}
        >>> columns_exists(df, ["b", "d"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        False
        ```
        !!! failure "Conclusion: One column is missing."
        </div>

        ```{.py .python linenums="1" title="Example 3: All columns missing"}
        >>> columns_exists(df, ["c", "d"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        False
        ```
        !!! failure "Conclusion: All columns are missing."
        </div>
    """
    return _columns_exists(dataframe, columns, match_case).result


@typechecked
def assert_column_exists(
    dataframe: psDataFrame,
    column: str,
    match_case: bool = False,
) -> None:
    """
    !!! note "Summary"
        Check whether a given `#!py column` exists as a valid column within `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check.
        column (str):
            The column to check.
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            If `#!py False`, will default to: `#!py column.upper()`.<br>
            Default: `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If `#!py column` does not exist within `#!py dataframe.columns`.

    Returns:
        (type(None)):
            Nothing is returned. Either an `#!py AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1,2,3,4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example 1: No error"}
        >>> assert_column_exists(df, "a")
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        None
        ```
        !!! success "Conclusion: Column exists."
        </div>

        ```{.py .python linenums="1" title="Example 2: Error raised"}
        >>> assert_column_exists(df, "c")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Error: Column "c" does not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>
    """
    if not column_exists(dataframe, column, match_case):
        raise AttributeError(
            f"Column '{column}' does not exist in 'dataframe'.\n"
            f"Try one of: {dataframe.columns}."
        )


@typechecked
def assert_columns_exists(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
    match_case: bool = False,
) -> None:
    """
    !!! note "Summary"
        Check whether all of the values in `#!py columns` exist in `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check
        columns (Union[str_list, str_tuple, str_set]):
            The columns to check
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            If `#!py False`, will default to: `#!py [col.upper() for col in columns]`.<br>
            Default: `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (type(None)):
            Nothing is returned. Either an `#!py AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example 1: No error"}
        >>> assert_columns_exists(df, ["a", "b"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        None
        ```
        !!! success "Conclusion: Columns exist."
        </div>

        ```{.py .python linenums="1" title="Example 2: One column missing"}
        >>> assert_columns_exists(df, ["b", "c"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Error: Columns ["c"] do not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Column "c" does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 3: Multiple columns missing"}
        >>> assert_columns_exists(df, ["b", "c", "d"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Error: Columns ["c", "d"] do not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Columns "c" and "d" does not exist."
        </div>
    """
    columns = list(columns) if is_type(columns, str) else columns
    (exist, missing_cols) = _columns_exists(dataframe, columns, match_case)
    if not exist:
        raise AttributeError(
            f"Columns {missing_cols} do not exist in 'dataframe'.\n"
            f"Try one of: {dataframe.columns}"
        )


@typechecked
def warn_column_missing(
    dataframe: psDataFrame,
    column: str,
    match_case: bool = False,
) -> None:
    """
    !!! summary "Summary"
        Check whether a given `#!py column` exists as a valid column within `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check.
        column (str):
            The column to check.
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            Defaults to `#!py False`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (type(None)):
            Nothing is returned. Either an `#!py AttributeWarning` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import warn_column_missing
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example 1: No error"}
        >>> warn_column_missing(df, ["a", "b"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        None
        ```
        !!! success "Conclusion: Columns exist."
        </div>

        ```{.py .python linenums="1" title="Example 2: Warning raised"}
        >>> warn_column_missing(df, "c")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Warning: Column "c" does not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>
    """
    if not column_exists(dataframe, column, match_case):
        warn(
            f"Column '{column}' does not exist in 'dataframe'.\n"
            f"Try one of: {dataframe.columns}.",
            AttributeWarning,
        )


@typechecked
def warn_columns_missing(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
    match_case: bool = False,
) -> None:
    """
    !!! summary "Summary"
        Check whether all of the values in `#!py columns` exist in `#!py dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check.
        columns (Union[str, str_collection]):
            The columns to check.
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            Defaults to `#!py False`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (type(None)):
            Nothing is returned. Either an `#!py AttributeWarning` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import warn_columns_missing
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Example 1: No error"}
        >>> warn_columns_missing(df, ["a", "b"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        None
        ```
        !!! success "Conclusion: Columns exist."
        </div>

        ```{.py .python linenums="1" title="Example 2: One column missing"}
        >>> warn_columns_missing(df, ["b", "c"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Warning: Columns ["c"] do not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Column "c" does not exist."
        </div>

        ```{.py .python linenums="1" title="Example 3: Multiple columns missing"}
        >>> warn_columns_missing(df, ["b", "c", "d"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Warning: Columns ["c", "d"] do not exist in "dataframe".
        Try one of: ["a", "b"].
        ```
        !!! failure "Conclusion: Columns "c" and "d" does not exist."
        </div>
    """
    columns = list(columns) if is_type(columns, str) else columns
    (exist, missing_cols) = _columns_exists(dataframe, columns, match_case)
    if not exist:
        warn(
            f"Columns {missing_cols} do not exist in 'dataframe'.\n"
            f"Try one of: {dataframe.columns}",
            AttributeWarning,
        )


# ---------------------------------------------------------------------------- #
#  Type checks                                                              ####
# ---------------------------------------------------------------------------- #


@typechecked
def is_vaid_spark_type(datatype: str) -> bool:
    """
    !!! note "Summary"
        Check whether a given `#!py datatype` is a correct and valid `#!py pyspark` data type.

    Params:
        datatype (str):
            The name of the data type to check.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the given `#!py datatype` is not a valid `#!py pyspark` data type.

    Returns:
        (type(None)):
            Nothing is returned. Either an `#!py AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> from toolbox_pyspark.checks import is_vaid_spark_type
        ```

        ```{.py .python linenums="1" title="Loop through all valid types"}
        >>> type_names = ["string", "char", "varchar", "binary", "boolean", "decimal", "float", "double", "byte", "short", "integer", "long", "date", "timestamp", "timestamp_ntz", "void"]
        >>> for type_name in type_names:
        ...     is_vaid_spark_type(type_name)
        ```
        <div class="result" markdown>
        Nothing is returned each time. Because they're all valid.
        !!! success "Conclusion: They're all valid."
        </div>

        ```{.py .python linenums="1" title="Check some invalid types"}
        >>> type_names = ["np.ndarray", "pd.DataFrame", "dict"]
        >>> for type_name in type_names:
        ...     is_vaid_spark_type(type_name)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        AttributeError: DataType 'np.ndarray' is not valid.
        Must be one of: ["binary", "bool", "boolean", "byte", "char", "date", "decimal", "double", "float", "int", "integer", "long", "short", "str", "string", "timestamp", "timestamp_ntz", "varchar", "void"]
        ```
        ```{.txt .text title="Terminal"}
        AttributeError: DataType 'pd.DataFrame' is not valid.
        Must be one of: ["binary", "bool", "boolean", "byte", "char", "date", "decimal", "double", "float", "int", "integer", "long", "short", "str", "string", "timestamp", "timestamp_ntz", "varchar", "void"]
        ```
        ```{.txt .text title="Terminal"}
        AttributeError: DataType 'dict' is not valid.
        Must be one of: ["binary", "bool", "boolean", "byte", "char", "date", "decimal", "double", "float", "int", "integer", "long", "short", "str", "string", "timestamp", "timestamp_ntz", "varchar", "void"]
        ```
        !!! failure "Conclusion: All of these types are invalid."
        </div>
    """
    return datatype in VALID_PYSPARK_TYPE_NAMES


@typechecked
def assert_valid_spark_type(datatype: str) -> None:
    if not is_vaid_spark_type(datatype):
        raise AttributeError(
            f"DataType '{datatype}' is not valid.\n"
            f"Must be one of: {VALID_PYSPARK_TYPE_NAMES}"
        )


# ---------------------------------------------------------------------------- #
#  Table Existence                                                          ####
# ---------------------------------------------------------------------------- #


@typechecked
def table_exists(
    name: str,
    path: str,
    data_format: str,
    spark_session: SparkSession,
) -> bool:
    """
    !!! note "Summary"
        Will try to read `#!py table` from `#!py path` using `#!py format`, and if successful will return `#!py True` otherwise `#!py False`.

    Params:
        name (str):
            The name of the table to check exists.
        path (str):
            The directory where the table should be existing.
        data_format (str):
            The format of the table to try checking.
        spark_session (SparkSession):
            The `#!py spark` session to use for the importing.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (bool):
            Returns `#!py True` if the table exists, `False` otherwise.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import write_to_path
        >>> from toolbox_pyspark.checks import table_exists
        >>>
        >>> # Constants
        >>> write_name = "test_df"
        >>> write_path = f"./test"
        >>> write_format = "parquet"
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
        ...         }
        ...     )
        ... )
        >>>
        >>> # Write data
        >>> write_to_path(df, f"{write_name}.{write_format}", write_path)
        ```

        ```{.py .python linenums="1" title="Example 1: Table exists"}
        >>> table_exists("test_df.parquet", "./test", "parquet", spark)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Table exists."
        </div>

        ```{.py .python linenums="1" title="Example 2: Table does not exist"}
        >>> table_exists("bad_table_name.parquet", "./test", "parquet", spark)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        False
        ```
        !!! failure "Conclusion: Table does not exist."
        </div>

    ???+ tip "See Also"
        - [`toolbox_pyspark.io.read_from_path()`][toolbox_pyspark.io.read_from_path]
    """
    try:
        _ = read_from_path(
            name=name,
            path=path,
            data_format=data_format,
            spark_session=spark_session,
        )
    except Exception:
        return False
    return True
