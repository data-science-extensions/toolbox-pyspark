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

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from toolbox_python.collection_types import str_list, str_set, str_tuple
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.constants import VALID_PYSPARK_TYPE_NAMES
from toolbox_pyspark.io import read_from_path


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "column_exists",
    "columns_exists",
    "is_vaid_spark_type",
    "table_exists",
    "assert_column_exists",
    "assert_columns_exists",
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
    columns: Union[str_list, str_tuple, str_set],
    match_case: bool = False,
) -> ColumnExistsResult:
    cols: Union[str_list, str_tuple, str_set] = (
        columns if match_case else [col.upper() for col in columns]
    )
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
        Check whether a given `column` exists as a valid column within `dataframe.columns`.

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
            True if exists or False.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Column Exists"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> result = column_exists(df, 'a')
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.py .python}
        True
        ```
        </div>

        ```{.py .python linenums="1" title="Column Missing"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> result = column_exists(df, 'c')
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.py .python}
        False
        ```
        </div>
    """
    return _columns_exists(dataframe, [column], match_case).result


@typechecked
def columns_exists(
    dataframe: psDataFrame,
    columns: Union[str_list, str_tuple, str_set],
    match_case: bool = False,
) -> bool:
    """
    !!! note "Summary"
        Check whether all of the values in `columns` exist in `dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check
        columns (Union[List[str], Set[str], Tuple[str, ...]]):
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
            True if exists or False.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Columns exist"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> columns_exists(df, ['a','b'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        True
        ```
        </div>

        ```{.py .python linenums="1" title="One column missing"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> columns_exists(df, ['b','d'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        False
        ```
        </div>

        ```{.py .python linenums="1" title="All columns missing"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> columns_exists(df, ['c','d'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        False
        ```
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
        Check whether a given `column` exists as a valid column within `dataframe.columns`.

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
            If `column` does not exist within `dataframe.columns`.

    Returns:
        (type(None)):
            Nothing is returned. Either an `AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="No error"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> assert_column_exists(df, 'a')
        ```
        <div class="result" markdown>
        ```{.py .python}
        None
        ```
        </div>

        ```{.py .python linenums="1" title="Error raised"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_column_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> assert_column_exists(df, 'c')
        ```
        <div class="result" markdown>
        ```{.txt .text}
        "Attribute Error: Column 'c' does not exist in 'dataframe'."
        "Try one of: ['a','b']."
        ```
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
    columns: Union[str_list, str_tuple, str_set],
    match_case: bool = False,
) -> None:
    """
    !!! note "Summary"
        Check whether all of the values in `columns` exist in `dataframe.columns`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to check
        columns (Union[List[str], Set[str], Tuple[str, ...]]):
            The columns to check
        match_case (bool, optional):
            Whether or not to match the string case for the columns.<br>
            If `#!py False`, will default to: `#!py [col.upper() for col in columns]`.<br>
            Default: `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the `columns` do not exist within `dataframe.columns`.

    Returns:
        (type(None)):
            Nothing is returned. Either an `AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="No error"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> assert_columns_exists(df, ['a','b'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        None
        ```
        </div>

        ```{.py .python linenums="1" title="One column missing"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> assert_columns_exists(df, ['b','c'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        "Attribute Error: Columns ['c'] do not exist in 'dataframe'."
        "Try one of: ['a','b']."
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns missing"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.checks import assert_columns_exists
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> assert_columns_exists(df, ['b','c','d'])
        ```
        <div class="result" markdown>
        ```{.txt .text}
        "Attribute Error: Columns ['c','d'] do not exist in 'dataframe'."
        "Try one of: ['a','b']."
        ```
        </div>
    """
    (exist, missing_cols) = _columns_exists(dataframe, columns, match_case)
    if not exist:
        raise AttributeError(
            f"Columns {missing_cols} do not exist in 'dataframe'.\n"
            f"Try one of: {dataframe.columns}"
        )


# ---------------------------------------------------------------------------- #
#  Type checks                                                              ####
# ---------------------------------------------------------------------------- #


@typechecked
def is_vaid_spark_type(datatype: str) -> None:
    """
    !!! note "Summary"
        Check whether a given `datatype` is a correct and valid `pyspark` data type.

    Params:
        datatype (str):
            The name of the data type to check.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the given `datatype` is not a valid `pyspark` data type.

    Returns:
        (type(None)):
            Nothing is returned. Either an `AttributeError` exception is raised, or nothing.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Loop through all valid types"}
        >>> from toolbox_pyspark.checks import is_vaid_spark_type
        >>> type_names = ['string', 'char', 'varchar', 'binary', 'boolean', 'decimal', 'float', 'double', 'byte', 'short', 'integer', 'long', 'date', 'timestamp', 'timestamp_ntz', 'void']
        >>> for type_name in type_names:
        ...     is_vaid_spark_type(type_name)
        ```
        <div class="result" markdown>
        Nothing is returned each time. Because they're all valid.
        </div>

        ```{.py .python linenums="1" title="Check some invalid types"}
        >>> from toolbox_pyspark.checks import is_vaid_spark_type
        >>> type_names = ["np.ndarray", "pd.DataFrame", "dict"]
        >>> for type_name in type_names:
        ...     is_vaid_spark_type(type_name)
        ```
        <div class="result" markdown>
        ```{.txt .text}
        AttributeError: DataType `np.ndarray` is not valid.
        Must be one of: ['binary', 'bool', 'boolean', 'byte', 'char', 'date', 'decimal', 'double', 'float', 'int', 'integer', 'long', 'short', 'str', 'string', 'timestamp', 'timestamp_ntz', 'varchar', 'void']
        ```

        ```{.txt .text}
        AttributeError: DataType `pd.DataFrame` is not valid.
        Must be one of: ['binary', 'bool', 'boolean', 'byte', 'char', 'date', 'decimal', 'double', 'float', 'int', 'integer', 'long', 'short', 'str', 'string', 'timestamp', 'timestamp_ntz', 'varchar', 'void']
        ```

        ```{.txt .text}
        AttributeError: DataType `dict` is not valid.
        Must be one of: ['binary', 'bool', 'boolean', 'byte', 'char', 'date', 'decimal', 'double', 'float', 'int', 'integer', 'long', 'short', 'str', 'string', 'timestamp', 'timestamp_ntz', 'varchar', 'void']
        ```
        </div>
    """
    if datatype not in VALID_PYSPARK_TYPE_NAMES:
        raise AttributeError(
            f"DataType `{datatype}` is not valid.\n"
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
        Will try to read `table` from `path` using `format`, and if successful will return `True` otherwise `False`.

    Params:
        name (str):
            The name of the table to check exists.
        path (str):
            The directory where the table should be existing.
        data_format (str):
            The format of the table to try checking.
        spark_session (SparkSession):
            The `spark` session to use for the importing.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (bool):
            Returns `True` if the table exists, `False` otherwise.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> write_name = "test_df"
        >>> write_path = f"./test"
        >>> write_format = "parquet"
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...     })
        ... )
        >>> (
        ...    df
        ...    .write
        ...    .mode("overwrite")
        ...    .format(write_format)
        ...    .save(f"{write_path}/{write_name}.{write_format}")
        ...)
        ```

        ```{.py .python linenums="1" title="Table exists"}
        >>> from toolbox_pyspark.checks import table_exists
        >>> table_exists("test_df.parquet", "./test", "parquet", spark)
        ```
        <div class="result" markdown>
        ```{.py .python}
        True
        ```
        </div>

        ```{.py .python linenums="1" title="Table does not exist"}
        >>> from toolbox_pyspark.checks import table_exists
        >>> table_exists("bad_table_name.parquet", "./test", "parquet", spark)
        ```
        <div class="result" markdown>
        ```{.py .python}
        False
        ```
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
