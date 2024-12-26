# ============================================================================ #
#                                                                              #
#     Title   : Dataframe Cleaning                                             #
#     Purpose : Clean, fix, and fetch various aspects on a given DataFrame.    #
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
    The `cleaning` module is used to clean, fix, and fetch various aspects on a given DataFrame.
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
from numpy import ndarray as npArray
from pandas import DataFrame as pdDataFrame
from pyspark.sql import (
    Column,
    DataFrame as psDataFrame,
    SparkSession,
    functions as F,
    types as T,
)
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_list
from toolbox_python.lists import flatten
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import assert_column_exists, assert_columns_exists
from toolbox_pyspark.columns import get_columns
from toolbox_pyspark.constants import (
    LITERAL_LIST_OBJECT_NAMES,
    LITERAL_NUMPY_ARRAY_NAMES,
    LITERAL_PANDAS_DATAFRAME_NAMES,
    LITERAL_PYSPARK_DATAFRAME_NAMES,
    VALID_LIST_OBJECT_NAMES,
    VALID_NUMPY_ARRAY_NAMES,
    VALID_PANDAS_DATAFRAME_NAMES,
    VALID_PYAPARK_JOIN_TYPES,
    VALID_PYSPARK_DATAFRAME_NAMES,
    WHITESPACE_CHARACTERS as WHITESPACES,
)


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "create_empty_dataframe",
    "keep_first_record_by_columns",
    "convert_dataframe",
    "get_column_values",
    "update_nullability",
    "trim_spaces_from_column",
    "trim_spaces_from_columns",
    "apply_function_to_column",
    "apply_function_to_columns",
    "drop_matching_rows",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Empty DataFrame                                                          ####
# ---------------------------------------------------------------------------- #


@typechecked
def create_empty_dataframe(spark_session: SparkSession) -> psDataFrame:
    return spark_session.createDataFrame([], T.StructType([]))


# ---------------------------------------------------------------------------- #
#  Column processes                                                         ####
# ---------------------------------------------------------------------------- #


@typechecked
def keep_first_record_by_columns(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given Spark `#!py DataFrame`, keep the first record given by the column(s) specified in `#!py columns`.

    ???+ abstract "Details"
        The necessity for this function arose when we needed to perform a `#!py distinct()` function for a given `#!py DataFrame`; however, we still wanted to retain data provided in the other columns.

    Params:
        dataframe (psDataFrame):
            The DataFrame that you want to filter.
        columns (Union[str, str_collection]):
            The single or multiple columns by which you want to extract the distinct values from.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated dataframe, retaining only the first unique set of records from the columns specified in `#!py columns`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import keep_first_record_by_columns
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
        ...             "c": [1, 1, 2, 2],
        ...             "d": [1, 2, 2, 2],
        ...             "e": [1, 1, 2, 3],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Distinct by the `c` column"}
        >>> keep_first_record_by_columns(df, "c").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 3 | c | 2 | 2 | 2 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `c` column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Distinct by the `d` column"}
        >>> keep_first_record_by_columns(df, "d").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `d` column."
        </div>

        ```{.py .python linenums="1" title="Example 3: Distinct by the `e` column"}
        >>> keep_first_record_by_columns(df, "e").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `e` column."
        </div>

        ```{.py .python linenums="1" title="Example 4: Distinct by the `c` & `d` columns"}
        >>> keep_first_record_by_columns(df, ["c", "d"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        | 3 | c | 2 | 2 | 2 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `c` & `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 5: Distinct by the `c` & `e` columns"}
        >>> keep_first_record_by_columns(df, ["c", "e"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `c` & `e` columns."
        </div>

        ```{.py .python linenums="1" title="Example 6: Distinct by the `d` & `e` columns"}
        >>> keep_first_record_by_columns(df, ["d", "e"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `d` & `e` columns."
        </div>

        ```{.py .python linenums="1" title="Example 7: Distinct by the `c`, `d` & `e` columns"}
        >>> keep_first_record_by_columns(df, ["c", "d", "e"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully kept first records by the `c`, `d` & `e` columns."
        !!! failure "Conclusion: Failure."
        </div>

        ```{.py .python linenums="1" title="Example 8: Column missing"}
        >>> keep_first_record_by_columns(df, "f")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column 'f' does not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d", "e"]
        ```
        !!! failure "Conclusion: Column missing."
        </div>

    ??? info "Notes"
        The way this process will retain only the first record in the given `#!py columns` is by:

        1. Add a new column called `RowNum`
            1. This `RowNum` column uses the SparkSQL function `#!sql ROW_NUMBER()`
            1. The window-function `#!sql OVER` clause will then:
                1. `#!sql PARTITION BY` the `#!py columns`,
                1. `#!sql ORDER BY` the `#!py columns`.
        1. Filter so that `#!sql RowNum=1`.
        1. Drop the `#!py RowNum` column.

    ???+ tip "See Also"
        - [`toolbox_pyspark.checks.assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
    """
    columns = [columns] if is_type(columns, str) else columns
    assert_columns_exists(dataframe, columns)
    return (
        dataframe.withColumn(
            colName="RowNum",
            col=F.expr(
                f"""
                ROW_NUMBER()
                OVER
                (
                    PARTITION BY {','.join(columns)}
                    ORDER BY {','.join(columns)}
                )
                """
            ),
        )
        .where("RowNum=1")
        .drop("RowNum")
    )


@typechecked
def convert_dataframe(
    dataframe: psDataFrame,
    return_type: Union[
        LITERAL_PYSPARK_DATAFRAME_NAMES,
        LITERAL_PANDAS_DATAFRAME_NAMES,
        LITERAL_NUMPY_ARRAY_NAMES,
        LITERAL_LIST_OBJECT_NAMES,
        str,
    ] = "pd",
) -> Optional[Union[psDataFrame, pdDataFrame, npArray, list]]:
    """
    !!! note "Summary"
        Convert a PySpark DataFrame to the desired return type.

    ???+ abstract "Details"
        This function converts a PySpark DataFrame to one of the supported return types, including:

        PySpark DataFrame:

        <div class="mdx-four-columns" markdown>

        - `#!py "spark.DataFrame"`
        - `#!py "pyspark.DataFrame"`
        - `#!py "pyspark"`
        - `#!py "spark"`
        - `#!py "ps.DataFrame"`
        - `#!py "ps.df"`
        - `#!py "psdf"`
        - `#!py "psDataFrame"`
        - `#!py "psDF"`
        - `#!py "ps"`

        </div>

        Pandas DataFrame:

        <div class="mdx-four-columns" markdown>

        - `#!py "pandas.DataFrame"`
        - `#!py "pandas"`
        - `#!py "pd.DataFrame"`
        - `#!py "pd.df"`
        - `#!py "pddf"`
        - `#!py "pdDataFrame"`
        - `#!py "pdDF"`
        - `#!py "pd"`

        </div>

        NumPy array:

        <div class="mdx-four-columns" markdown>

        - `#!py "numpy.array"`
        - `#!py "np.array"`
        - `#!py "np"`
        - `#!py "numpy"`
        - `#!py "nparr"`
        - `#!py "npa"`
        - `#!py "np.arr"`
        - `#!py "np.a"`

        </div>

        Python list:

        <div class="mdx-four-columns" markdown>

        - `#!py "list"`
        - `#!py "lst"`
        - `#!py "l"`
        - `#!py "flat_list"`
        - `#!py "flatten_list"`

        </div>

    Params:
        dataframe (psDataFrame):
            The PySpark DataFrame to be converted.
        return_type (Union[LITERAL_LIST_OBJECT_NAMES, LITERAL_PANDAS_DATAFRAME_NAMES, LITERAL_PYSPARK_DATAFRAME_NAMES, LITERAL_NUMPY_ARRAY_NAMES, str], optional):
            The desired return type.<br>
            Options:

            - `#!py "ps"`: Return the PySpark DataFrame.
            - `#!py "pd"`: Return a Pandas DataFrame.
            - `#!py "np"`: Return a NumPy array.
            - `#!py "list"`: Return a Python list.
            - `#!py "list_flat"`: Return a flat Python list (1D).

            Defaults to `#!py "pd"` (Pandas DataFrame).

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValueError:
            If any of the values parsed to `return_type` are not valid options.

    Returns:
        (Optional[Union[psDataFrame, pdDataFrame, npArray, list]]):
            The converted data in the specified return type.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import convert_dataframe
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pdDataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+
        | a | b |
        +---+---+
        | 0 | a |
        | 1 | b |
        | 2 | c |
        | 3 | d |
        +---+---+
        ```

        ```{.py .python linenums="1" title="Example 1: Convert to PySpark"}
        >>> new_df = convert_dataframe(df, "ps")
        >>> print(type(new_df))
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'pyspark.sql.dataframe.DataFrame'>
        ```
        ```{.txt .text title="Terminal"}
        +---+---+
        | a | b |
        +---+---+
        | 0 | a |
        | 1 | b |
        | 2 | c |
        | 3 | d |
        +---+---+
        ```
        !!! success "Conclusion: Successfully converted to PySpark."
        </div>

        ```{.py .python linenums="1" title="Example 2: Convert to Pandas"}
        >>> new_df = convert_dataframe(df, "pd")
        >>> print(type(new_df))
        >>> print(new_df)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'pandas.core.frame.DataFrame'>
        ```
        ```{.txt .text title="Terminal"}
           a  b
        0  0  a
        1  1  b
        2  2  c
        3  3  d
        ```
        !!! success "Conclusion: Successfully converted to Pandas."
        </div>

        ```{.py .python linenums="1" title="Example 3: Convert to Numpy"}
        >>> new_df = convert_dataframe(df, "np")
        >>> print(type(new_df))
        >>> print(new_df)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'numpy.ndarray'>
        ```
        ```{.txt .text title="Terminal"}
        [[0 "a"]
         [1 "b"]
         [2 "c"]
         [3 "d"]]
        ```
        !!! success "Conclusion: Successfully converted to Numpy."
        </div>

        ```{.py .python linenums="1" title="Example 4: List"}
        >>> new_df = convert_dataframe(df, "list")
        >>> print(type(new_df))
        >>> print(new_df)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'list'>
        ```
        ```{.txt .text title="Terminal"}
        [
            [0, "a"],
            [1, "b"],
            [2, "c"],
            [3, "d"],
        ]
        ```
        !!! success "Conclusion: Successfully converted to List."
        </div>

        ```{.py .python linenums="1" title="Example 5: Convert to single column as list"}
        >>> new_df = convert_dataframe(df.select("b"), "flat_list")
        >>> print(type(new_df))
        >>> print(new_df)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'list'>
        ```
        ```{.txt .text title="Terminal"}
        ["a", "b", "c", "d"]
        ```
        !!! success "Conclusion: Successfully converted to flat List."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid return type"}
        >>> convert_dataframe(df, "invalid")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ValueError: Unknown return type: 'invalid'.
        Must be one of: ['pd', 'ps', 'np', 'list'].
        For more info, check the `constants` module.
        ```
        !!! failure "Conclusion: Invalid return type."
        </div>

    ???+ tip "See Also"
        - [`toolbox_pyspark.constants`][toolbox_pyspark.constants]
    """
    if return_type in VALID_PYSPARK_DATAFRAME_NAMES:
        return dataframe
    elif return_type in VALID_PANDAS_DATAFRAME_NAMES:
        return dataframe.toPandas()
    elif return_type in VALID_NUMPY_ARRAY_NAMES:
        return dataframe.toPandas().values  # type:ignore
    elif return_type in VALID_LIST_OBJECT_NAMES:
        if "flat" in return_type:
            return flatten(dataframe.toPandas().values.tolist())  # type:ignore
        else:
            return dataframe.toPandas().values.tolist()  # type:ignore
    else:
        raise ValueError(
            f"Unknown return type: '{return_type}'.\n"
            f"Must be one of: {['pd', 'ps', 'np', 'list']}.\n"
            f"For more info, check the `constants` module."
        )


@typechecked
def get_column_values(
    dataframe: psDataFrame,
    column: str,
    distinct: bool = True,
    return_type: Union[
        LITERAL_PYSPARK_DATAFRAME_NAMES,
        LITERAL_PANDAS_DATAFRAME_NAMES,
        LITERAL_NUMPY_ARRAY_NAMES,
        LITERAL_LIST_OBJECT_NAMES,
        str,
    ] = "pd",
) -> Optional[Union[psDataFrame, pdDataFrame, npArray, list]]:
    """
    !!! note "Summary"
        Extract and return unique values from a specified column of a PySpark DataFrame.

    ???+ abstract "Details"
        This function retrieves the unique values from a specified column of a PySpark DataFrame.

    Params:
        dataframe (psDataFrame):
            The input PySpark DataFrame.
        column (str):
            The name of the column from which to extract unique values.
        distinct (bool, optional):
            If `#!py True`, return distinct (unique) values.<br>
            Defaults to `#!py True`.
        return_type (Union[LITERAL_LIST_OBJECT_NAMES, LITERAL_PANDAS_DATAFRAME_NAMES, LITERAL_PYSPARK_DATAFRAME_NAMES, LITERAL_NUMPY_ARRAY_NAMES, str], optional):
            The desired return type.<br>
            Options:

            - `#!py "ps"`: Return the result as a PySpark DataFrame.
            - `#!py "pd"`: Return the result as a Pandas DataFrame.
            - `#!py "np"`: Return the result as a NumPy array.
            - `#!py "list"`: Return the result as a Python list.

            Defaults to `#!py "pd"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValueError:
            If any of the values parsed to `return_type` are not valid options.
        ColumnDoesNotExistError:
            If the `#!py column` does not exist within `#!py dataframe.columns`.

    Returns:
        (Optional[Union[psDataFrame, pdDataFrame, npArray, list]]):
            The values from the given column, in the desired type.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import get_column_values
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ["c", "c", "c", "c"],
        ...             "d": ["d", "d", "d", "d"],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | d |
        | 1 | b | c | d |
        | 2 | c | c | d |
        | 3 | d | c | d |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default params"}
        >>> values = get_column_values(df, "c")
        >>> print(type(values))
        >>> print(values)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'pandas.core.frame.DataFrame'>
        ```
        ```{.txt .text title="Terminal"}
          c
        0 c
        ```
        !!! success "Conclusion: Successfully extracted distinct values from the `c` column, and converted to Pandas."
        </div>

        ```{.py .python linenums="1" title="Example 2: Not distinct"}
        >>> values = get_column_values(df, "c", False)
        >>> print(type(values))
        >>> print(values)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'pandas.core.frame.DataFrame'>
        ```
        ```{.txt .text title="Terminal"}
          c
        0 c
        1 c
        2 c
        3 c
        ```
        !!! success "Conclusion: Successfully extracted values from the `c` column, and converted to Pandas."
        </div>

        ```{.py .python linenums="1" title="Example 3: Flat list"}
        >>> values = get_column_values(df, "c", False, "flat_list")
        >>> print(type(values))
        >>> print(values)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        <class 'list'>
        ```
        ```{.txt .text title="Terminal"}
        ["c", "c", "c", "c"]
        ```
        !!! success "Conclusion: Successfully extracted values from the `c` column, and converted to flat List."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid return type"}
        >>> get_column_values(df, "c", return_type="invalid")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ValueError: Unknown return type: 'invalid'.
        Must be one of: ['pd', 'ps', 'np', 'list'].
        For more info, check the `constants` module.
        ```
        !!! failure "Conclusion: Invalid return type."
        </div>

    ???+ tip "See Also"
        - [`toolbox_pyspark.cleaning.convert_dataframe()`][toolbox_pyspark.cleaning.convert_dataframe]
        - [`toolbox_pyspark.constants`][toolbox_pyspark.constants]
    """
    df: psDataFrame = dataframe.select(column).filter(
        f"{column} is not null and {column} <> ''"
    )
    df = df.distinct() if distinct else df
    return convert_dataframe(dataframe=df, return_type=return_type)


@typechecked
def update_nullability(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
    nullable: bool = True,
) -> psDataFrame:
    """
    !!! note "Summary"
        Update the nullability of specified columns in a PySpark DataFrame.

    ???+ abstract "Details"
        This function updates the nullability of the specified columns in a PySpark DataFrame. If no columns are specified, it updates the nullability of all columns.

    Params:
        dataframe (psDataFrame):
            The input PySpark DataFrame.
        columns (Optional[Union[str, str_collection]], optional):
            The columns for which to update nullability. If not provided, all columns will be updated.<br>
            Defaults to `#!py None`.
        nullable (bool, optional):
            Whether to set the columns as nullable or not.<br>
            Defaults to `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated DataFrame with the specified columns' nullability updated.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import update_nullability
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
        ...             "c": [1.1, 2.2, 3.3, 4.4],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        >>> print(df.schema)
        >>> df.printSchema()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+
        | a | b |   c |
        +---+---+-----+
        | 1 | a | 1.1 |
        | 2 | b | 2.2 |
        | 3 | c | 3.3 |
        | 4 | d | 4.4 |
        +---+---+-----+
        ```
        ```{.sh .shell title="Terminal"}
        StructType(
            [
                StructField("a", LongType(), True),
                StructField("b", StringType(), True),
                StructField("c", DoubleType(), True),
            ]
        )
        ```
        ```{.txt .text title="Terminal"}
        root
         |-- a: long (nullable = true)
         |-- b: string (nullable = true)
         |-- c: double (nullable = true)
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Update nullability of all columns"}
        >>> new_df = update_nullability(df, nullable=False)
        >>> new_df.printSchema()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        root
         |-- a: long (nullable = false)
         |-- b: string (nullable = false)
         |-- c: double (nullable = false)
        ```
        !!! success "Conclusion: Successfully updated nullability of all columns."
        </div>

        ```{.py .python linenums="1" title="Example 2: Update nullability of specific columns"}
        >>> new_df = update_nullability(df, columns=["a", "c"], nullable=False)
        >>> new_df.printSchema()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        root
         |-- a: long (nullable = false)
         |-- b: string (nullable = true)
         |-- c: double (nullable = false)
        ```
        !!! success "Conclusion: Successfully updated nullability of specific columns."
        </div>

        ```{.py .python linenums="1" title="Example 3: Column does not exist"}
        >>> update_nullability(df, columns="d", nullable=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column 'd' does not exist in the DataFrame.
        Try one of: ["a", "b", "c"]
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

    ??? success "Credit"
        All credit goes to: https://stackoverflow.com/questions/46072411/can-i-change-the-nullability-of-a-column-in-my-spark-dataframe#answer-51821437.

    ???+ tip "See Also"
        - [`toolbox_pyspark.checks.assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
    """
    columns = get_columns(dataframe, columns)
    assert_columns_exists(dataframe=dataframe, columns=columns)
    schema: T.StructType = dataframe.schema
    for struct_field in schema:
        if struct_field.name in columns:
            struct_field.nullable = nullable
    return dataframe.sparkSession.createDataFrame(data=dataframe.rdd, schema=dataframe.schema)


# ---------------------------------------------------------------------------- #
#  Trimming                                                                 ####
# ---------------------------------------------------------------------------- #


@typechecked
def trim_spaces_from_column(
    dataframe: psDataFrame,
    column: str,
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given list of columns, trim all of the excess white spaces from them.

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        column (str):
            The column to clean.

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
        >>> from toolbox_pyspark.cleaning import trim_spaces_from_column
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
        ...             "c": ["1   ", "1   ", "1   ", "1   "],
        ...             "d": ["   2", "   2", "   2", "   2"],
        ...             "e": ["   3   ", "   3   ", "   3   ", "   3   "],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+------+------+---------+
        | a | b |    c |    d |       e |
        +---+---+------+------+---------+
        | 1 | a | 1    |    2 |    3    |
        | 2 | b | 1    |    2 |    3    |
        | 3 | c | 1    |    2 |    3    |
        | 4 | d | 1    |    2 |    3    |
        +---+---+------+------+---------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Trim column"}
        >>> trim_spaces_from_column(df, "c").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+------+--------+
        | a | b | c |    d |      e |
        +---+---+---+------+--------+
        | 1 | a | 1 |    2 |   2    |
        | 2 | b | 1 |    2 |   2    |
        | 3 | c | 1 |    2 |   2    |
        | 4 | d | 1 |    2 |   2    |
        +---+---+---+------+--------+
        ```
        !!! success "Conclusion: Successfully trimmed the `c` column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid column"}
        >>> trim_spaces_from_column(df, "f")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column 'f' does not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d", "e"]
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

    ??? info "Notes"

        ???+ info "Justification"
            - The main reason for this function is because when the data was exported from the Legacy WMS's, there's a _whole bunch_ of trailing spaces in the data fields. My theory is because of the data type in the source system. That is, if it's originally stored as 'char' type, then it will maintain the data length. This issues doesn't seem to be affecting the `varchar` fields. Nonetheless, this function will strip the white spaces from the data; thus reducing the total size of the data stored therein.
            - The reason why it is necessary to write this out as a custom function, instead of using the [`F.trim()`][trim] function from the PySpark library directly is due to the deficiencies of the Java [`trim()`](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#trim) function. More specifically, there are 13 different whitespace characters available in our ascii character set. The Java function only cleans about 6 of these. So therefore, we define this function which iterates through all 13 whitespace characters, and formats them in to a regular expression, to then parse it to the [`F.regexp_replace()`][regexp_replace] function to be replaced with an empty string (`""`). Therefore, all 13 characters will be replaced, the strings will be cleaned and trimmed ready for further processing.

            [trim]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html
            [regexp_replace]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html

        ???+ info "Regex definition: `^[...]+|[...]+$`"
            - 1st Alternative: '^[...]+'
                - '^' asserts position at start of a line
                - Match a single character present in the list below '[...]'
                    - '+' matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)
                    - matches a single character in the list '  ' (case sensitive)
                        - matches the character ' ' with index 160 (A0 or 240) literally (case sensitive)
                        - matches the character ' ' with index 32 (20 or 40) literally (case sensitive)
                        - ... (repeat for all whitespace characters)
            - 2nd Alternative: '[...]+$'
                - Match a single character present in the list below '[...]'
                    - '+' matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)
                    - matches a single character in the list '  ' (case sensitive)
                        - matches the character ' ' with index 160 (A0 or 240) literally (case sensitive)
                        - matches the character ' ' with index 32 (20 or 40) literally (case sensitive)
                        - ... (repeat for all whitespace characters)
                - '$' asserts position at the end of a line

    ??? tip "See Also"
        - [`trim_spaces_from_columns()`][toolbox_pyspark.cleaning.trim_spaces_from_columns]
        - [`ALL_WHITESPACE_CHARACTERS`][toolbox_pyspark.constants.ALL_WHITESPACE_CHARACTERS]
    """
    assert_column_exists(dataframe=dataframe, column=column, match_case=True)
    space_chars: str_list = [chr(char.ascii) for char in WHITESPACES]
    regexp: str = f"^[{''.join(space_chars)}]+|[{''.join(space_chars)}]+$"
    return dataframe.withColumn(column, F.regexp_replace(column, regexp, ""))


@typechecked
def trim_spaces_from_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given list of columns, trim all of the excess white spaces from them.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be updated.
        columns (Optional[Union[str, str_collection]], optional):
            The list of columns to be updated.
            Must be valid columns on `dataframe`.
            If given as a string, will be executed as a single column (ie. one-element long list).
            If not given, will apply to all columns in `dataframe` which have the data-type `string`.
            It is also possible to parse the values `#!py "all"` or `#!py "all_string"`, which will also apply this function to all columns in `dataframe` which have the data-type `string`.<br>
            Defaults to `#!py None`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import trim_spaces_from_columns
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
        ...             "c": ["1   ", "1   ", "1   ", "1   "],
        ...             "d": ["   2", "   2", "   2", "   2"],
        ...             "e": ["   3   ", "   3   ", "   3   ", "   3   "],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+------+------+---------+
        | a | b |    c |    d |       e |
        +---+---+------+------+---------+
        | 1 | a | 1    |    2 |    3    |
        | 2 | b | 1    |    2 |    3    |
        | 3 | c | 1    |    2 |    3    |
        | 4 | d | 1    |    2 |    3    |
        +---+---+------+------+---------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: One column as list"}
        >>> trim_spaces_from_columns(df, ["c"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+------+---------+
        | a | b | c |    d |       e |
        +---+---+---+------+---------+
        | 1 | a | 1 |    2 |    3    |
        | 2 | b | 1 |    2 |    3    |
        | 3 | c | 1 |    2 |    3    |
        | 4 | d | 1 |    2 |    3    |
        +---+---+---+------+---------+
        ```
        !!! success "Conclusion: Successfully trimmed the `c` column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Single column as string"}
        >>> trim_spaces_from_columns(df, "d").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+------+---+---------+
        | a | b |    c | d |       e |
        +---+---+------+---+---------+
        | 1 | a | 1    | 2 |    3    |
        | 2 | b | 1    | 2 |    3    |
        | 3 | c | 1    | 2 |    3    |
        | 4 | d | 1    | 2 |    3    |
        +---+---+------+---+---------+
        ```
        !!! success "Conclusion: Successfully trimmed the `d` column."
        </div>

        ```{.py .python linenums="1" title="Example 3: Multiple columns"}
        >>> trim_spaces_from_columns(df, ["c", "d"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---------+
        | a | b | c | d |       e |
        +---+---+---+---+---------+
        | 1 | a | 1 | 2 |    3    |
        | 2 | b | 1 | 2 |    3    |
        | 3 | c | 1 | 2 |    3    |
        | 4 | d | 1 | 2 |    3    |
        +---+---+---+---+---------+
        ```
        !!! success "Conclusion: Successfully trimmed the `c` and `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 4: All columns"}
        >>> trim_spaces_from_columns(df, "all").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 2 | 3 |
        | 2 | b | 1 | 2 | 3 |
        | 3 | c | 1 | 2 | 3 |
        | 4 | d | 1 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully trimmed all columns."
        </div>

        ```{.py .python linenums="1" title="Example 5: Default config"}
        >>> trim_spaces_from_columns(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 2 | 3 |
        | 2 | b | 1 | 2 | 3 |
        | 3 | c | 1 | 2 | 3 |
        | 4 | d | 1 | 2 | 3 |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully trimmed all columns."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid column"}
        >>> trim_spaces_from_columns(df, ["f"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ['f'] do not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d", "e"]
        ```
        !!! failure "Conclusion: Columns do not exist."
        </div>

    ???+ info "Notes"

        ???+ info "Justification"
            - The main reason for this function is because when the data was exported from the Legacy WMS's, there's a _whole bunch_ of trailing spaces in the data fields. My theory is because of the data type in the source system. That is, if it's originally stored as 'char' type, then it will maintain the data length. This issues doesn't seem to be affecting the `varchar` fields. Nonetheless, this function will strip the white spaces from the data; thus reducing the total size of the data stored therein.
            - The reason why it is necessary to write this out as a custom function, instead of using the [`F.trim()`][trim] function from the PySpark library directly is due to the deficiencies of the Java [`trim()`](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#trim) function. More specifically, there are 13 different whitespace characters available in our ascii character set. The Java function only cleans about 6 of these. So therefore, we define this function which iterates through all 13 whitespace characters, and formats them in to a regular expression, to then parse it to the [`F.regexp_replace()`][regexp_replace] function to be replaced with an empty string (`""`). Therefore, all 13 characters will be replaced, the strings will be cleaned and trimmed ready for further processing.
            - The reason why this function exists as a standalone, and does not call [`trim_spaces_from_column()`][toolbox_pyspark.cleaning.trim_spaces_from_column] from within a loop is because [`trim_spaces_from_column()`][toolbox_pyspark.cleaning.trim_spaces_from_column] utilises the [`.withColumn()`][withColumn] method to implement the [`F.regexp_replace()`][regexp_replace] function on columns individually. When implemented iteratively, this process will create huge DAG's for the RDD, and blow out the complexity to a huge extend. Whereas this [`trim_spaces_from_columns()`][toolbox_pyspark.cleaning.trim_spaces_from_columns] function will utilise the [`.withColumns()`][withColumns] method to implement the [`F.regexp_replace()`][regexp_replace] function over all columns at once. This [`.withColumns()`][withColumns] method projects the function down to the underlying dataset in one single execution; not a different execution per column. Therefore, it is more simpler and more efficient.

            [withColumn]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
            [withColumns]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html
            [trim]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html
            [regexp_replace]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html

        ???+ info "Regex definition: `^[...]+|[...]+$`"
            - 1st Alternative: `^[...]+`
                - `^` asserts position at start of a line
                - Match a single character present in the list below `[...]`
                    - `+` matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)
                    - matches a single character in the list `  ` (case sensitive)
                        - matches the character ` ` with index 160 (A0 or 240) literally (case sensitive)
                        - matches the character ` ` with index 32 (20 or 40) literally (case sensitive)
                        - ... (repeat for all whitespace characters)
            - 2nd Alternative: `[...]+$`
                - Match a single character present in the list below `[...]`
                    - `+` matches the previous token between one and unlimited times, as many times as possible, giving back as needed (greedy)
                    - matches a single character in the list `  ` (case sensitive)
                        - matches the character ` ` with index 160 (A0 or 240) literally (case sensitive)
                        - matches the character ` ` with index 32 (20 or 40) literally (case sensitive)
                        - ... (repeat for all whitespace characters)

    ??? tip "See Also"
        - [`trim_spaces_from_column()`][toolbox_pyspark.cleaning.trim_spaces_from_column]
        - [`ALL_WHITESPACE_CHARACTERS`][toolbox_pyspark.constants.ALL_WHITESPACE_CHARACTERS]
    """
    columns = get_columns(dataframe, columns)
    assert_columns_exists(dataframe=dataframe, columns=columns, match_case=True)
    space_chars: str_list = WHITESPACES.to_list("chr")  # type:ignore
    regexp: str = f"^[{''.join(space_chars)}]+|[{''.join(space_chars)}]+$"
    cols_exprs: dict[str, Column] = {col: F.regexp_replace(col, regexp, "") for col in columns}
    return dataframe.withColumns(cols_exprs)


# ---------------------------------------------------------------------------- #
#  Applying functions                                                       ####
# ---------------------------------------------------------------------------- #


@typechecked
def apply_function_to_column(
    dataframe: psDataFrame,
    column: str,
    function: str = "upper",
    *function_args,
    **function_kwargs,
) -> psDataFrame:
    """
    !!! note "Summary"
        Apply a given PySpark `function` to a single `column` on `dataframe`.

    ???+ abstract "Details"
        Under the hood, this function will simply call the [`.withColumn()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html) method to apply the function named in `function` from the PySpark [`functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) module.
        ```py
        return dataframe.withColumn(column, getattr(F, function)(column, *function_args, **function_kwargs))
        ```

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        column (str):
            The column to update.
        function (str, optional):
            The function to execute. Must be a valid function from the PySpark [`functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) module.<br>
            Defaults to `#!py "upper"`.
        *function_args (Any, optional):
            The arguments to push down to the underlying `function`.
        **function_kwargs (Any, optional):
            The keyword arguments to push down to the underlying `function`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import apply_function_to_column
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ["c", "c", "c", "c"],
        ...             "d": ["d", "d", "d", "d"],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | d |
        | 1 | b | c | d |
        | 2 | c | c | d |
        | 3 | d | c | d |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default params"}
        >>> apply_function_to_column(df, "c").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | C | d |
        | 1 | b | C | d |
        | 2 | c | C | d |
        | 3 | d | C | d |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `upper` function to the `c` column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Simple function"}
        >>> apply_function_to_column(df, "c", "lower").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | d |
        | 1 | b | c | d |
        | 2 | c | c | d |
        | 3 | d | c | d |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `lower` function to the `c` column."
        </div>

        ```{.py .python linenums="1" title="Example 3: Complex function, using args"}
        >>> apply_function_to_column(df, "d", "lpad", 5, "?").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+-------+
        | a | b | c |     d |
        +---+---+---+-------+
        | 0 | a | c | ????d |
        | 1 | b | c | ????d |
        | 2 | c | c | ????d |
        | 3 | d | c | ????d |
        +---+---+---+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `d` column."
        </div>

        ```{.py .python linenums="1" title="Example 4: Complex function, using kwargs"}
        >>> new_df = apply_function_to_column(
        ...     dataframe=df,
        ...     column="d",
        ...     function="lpad",
        ...     len=5,
        ...     pad="?",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+-------+
        | a | b | c |     d |
        +---+---+---+-------+
        | 0 | a | c | ????d |
        | 1 | b | c | ????d |
        | 2 | c | c | ????d |
        | 3 | d | c | ????d |
        +---+---+---+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `d` column."
        </div>

        ```{.py .python linenums="1" title="Example 5: Different complex function, using kwargs"}
        >>> new_df = apply_function_to_column(
        ...     dataframe=df,
        ...     column="b",
        ...     function="regexp_replace",
        ...     pattern="c",
        ...     replacement="17",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----+---+---+
        | a |  b | c | d |
        +---+----+---+---+
        | 0 |  a | c | d |
        | 1 |  b | c | d |
        | 2 | 17 | c | d |
        | 3 |  d | c | d |
        +---+----+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `regexp_replace` function to the `b` column."
        </div>

        ```{.py .python linenums="1" title="Example 6: Part of pipe"}
        >>> new_df = df.transform(
        ...     func=apply_function_to_column,
        ...     column="d",
        ...     function="lpad",
        ...     len=5,
        ...     pad="?",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+-------+
        | a | b | c |     d |
        +---+---+---+-------+
        | 0 | a | c | ????d |
        | 1 | b | c | ????d |
        | 2 | c | c | ????d |
        | 3 | d | c | ????d |
        +---+---+---+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `d` column."
        </div>

        ```{.py .python linenums="1" title="Example 7: Column name in different case"}
        >>> new_df = df.transform(
        ...     func=apply_function_to_column,
        ...     column="D",
        ...     function="upper",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | D |
        | 1 | b | c | D |
        | 2 | c | c | D |
        | 3 | d | c | D |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `upper` function to the `D` column."
        </div>

        ```{.py .python linenums="1" title="Example 8: Invalid column"}
        >>> apply_function_to_column(df, "f")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column 'f' does not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d"]
        ```
        !!! failure "Conclusion: Column does not exist."
        </div>

    ??? info "Notes"
        - We have to name the `function` parameter as the full name because when this function is executed as part of a chain (by using the PySpark [`.transform()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html) method), that one uses the `func` parameter.

    ??? tip "See Also"
        - [`apply_function_to_columns()`][toolbox_pyspark.cleaning.apply_function_to_columns]
    """
    assert_column_exists(dataframe, column, False)
    return dataframe.withColumn(
        colName=column,
        col=getattr(F, function)(column, *function_args, **function_kwargs),
    )


@typechecked
def apply_function_to_columns(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
    function: str = "upper",
    *function_args,
    **function_kwargs,
) -> psDataFrame:
    """
    !!! note "Summary"
        Apply a given PySpark `function` over multiple `columns` on a given `dataframe`.

    ???+ abstract "Details"
        Under the hood, this function will simply call the [`.withColumns()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html) method to apply the function named in `function` from the PySpark [`functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) module.
        ```py
        return dataframe.withColumns(
            {column: getattr(F, function)(column, *args, **kwargs) for column in columns}
        )
        ```

    Params:
        dataframe (psDataFrame):
            The DataFrame to update.
        columns (Union[str, str_collection]):
            The columns to update.
        function (str, optional):
            The function to use.<br>
            Defaults to `#!py "upper"`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import apply_function_to_columns
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ["c", "c", "c", "c"],
        ...             "d": ["d", "d", "d", "d"],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | d |
        | 1 | b | c | d |
        | 2 | c | c | d |
        | 3 | d | c | d |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default params"}
        >>> apply_function_to_columns(df, ["b", "c"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | A | C | d |
        | 1 | B | C | d |
        | 2 | C | C | d |
        | 3 | D | C | d |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `upper` function to the `b` and `c` columns."
        </div>

        ```{.py .python linenums="1" title="Example 2: Simple function"}
        >>> apply_function_to_columns(df, ["b", "c"], "lower").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | a | c | d |
        | 1 | b | c | d |
        | 2 | c | c | d |
        | 3 | d | c | d |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `lower` function to the `b` and `c` columns."
        </div>

        ```{.py .python linenums="1" title="Example 3: Complex function, with args"}
        >>> apply_function_to_columns(df, ["b", "c", "d"], "lpad", 5, "?").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+-------+-------+-------+
        | a |     b |     c |     d |
        +---+-------+-------+-------+
        | 0 | ????a | ????c | ????d |
        | 1 | ????b | ????c | ????d |
        | 2 | ????c | ????c | ????d |
        | 3 | ????d | ????c | ????d |
        +---+-------+-------+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `b`, `c` and `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 4: Complex function, with kwargs"}
        >>> apply_function_to_columns(
        ...     dataframe=df,
        ...     columns=["b", "c", "d"],
        ...     function="lpad",
        ...     len=5,
        ...     pad="?",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+-------+-------+-------+
        | a |     b |     c |     d |
        +---+-------+-------+-------+
        | 0 | ????a | ????c | ????d |
        | 1 | ????b | ????c | ????d |
        | 2 | ????c | ????c | ????d |
        | 3 | ????d | ????c | ????d |
        +---+-------+-------+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `b`, `c` and `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 5: Different complex function, with kwargs"}
        >>> apply_function_to_columns(
        ...     dataframe=df,
        ...     columns=["b", "c", "d"],
        ...     function="regexp_replace",
        ...     pattern="c",
        ...     replacement="17",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----+----+---+
        | a |  b |  c | d |
        +---+----+----+---+
        | 0 |  a | 17 | d |
        | 1 |  b | 17 | d |
        | 2 | 17 | 17 | d |
        | 3 |  d | 17 | d |
        +---+----+----+---+
        ```
        !!! success "Conclusion: Successfully applied the `regexp_replace` function to the `b`, `c` and `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 6: Part of pipe"}
        >>> df.transform(
        ...     func=apply_function_to_columns,
        ...     columns=["b", "c", "d"],
        ...     function="lpad",
        ...     len=5,
        ...     pad="?",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+-------+-------+-------+
        | a |     b |     c |     d |
        +---+-------+-------+-------+
        | 0 | ????a | ????c | ????d |
        | 1 | ????b | ????c | ????d |
        | 2 | ????c | ????c | ????d |
        | 3 | ????d | ????c | ????d |
        +---+-------+-------+-------+
        ```
        !!! success "Conclusion: Successfully applied the `lpad` function to the `b`, `c` and `d` columns."
        </div>

        ```{.py .python linenums="1" title="Example 7: Column name in different case"}
        >>> apply_function_to_columns(
        ...     dataframe=df,
        ...     columns=["B", "c", "D"],
        ...     function="upper",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 0 | A | C | D |
        | 1 | B | C | D |
        | 2 | C | C | D |
        | 3 | D | C | D |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully applied the `upper` function to the `B`, `c` and `D` columns."
        </div>

        ```{.py .python linenums="1" title="Example 8: Invalid columns"}
        >>> apply_function_to_columns(df, ["f"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ['f'] do not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d"]
        ```
        !!! failure "Conclusion: Columns do not exist."
        </div>

    ??? info "Notes"
        - We have to name the `function` parameter as the full name because when this function is executed as part of a chain (by using the PySpark [`.transform()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html) method), that one uses the `func` parameter.

    ??? tip "See Also"
        - [`apply_function_to_column()`][toolbox_pyspark.cleaning.apply_function_to_column]
    """
    columns = get_columns(dataframe, columns)
    assert_columns_exists(dataframe, columns, False)
    return dataframe.withColumns(
        {
            column: getattr(F, function)(column, *function_args, **function_kwargs)
            for column in columns
        }
    )


# ---------------------------------------------------------------------------- #
#  Clean across tables                                                      ####
# ---------------------------------------------------------------------------- #


@typechecked
def drop_matching_rows(
    left_table: psDataFrame,
    right_table: psDataFrame,
    on_keys: Union[str, str_collection],
    join_type: VALID_PYAPARK_JOIN_TYPES = "left_anti",
    where_clause: Optional[str] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        This function is designed to _remove_ any rows on the `left_table` which _are_ existing on the `right_table`. That's why the `join_type` should always be `left_anti`.

    ???+ abstract "Details"
        The intention behind this function is originating from the `Accumulation` layer in the BigDaS environment. The process on this table layer is to only _insert_ rows from the `left_table` to the `right_table` with are **not existing** on the `right_table`. We include the `where_clause` here so that we can control any updated rows. Specifically, we check the `editdatetime` field between the `left_table` and the `right_table`, and any record on the `left_table` where the `editdatetime` field is _greater than_ the `editdatetime` value on the `right_table`, then this row will _remain_ on the `left_table`, and will later be _updated_ on the `right_table`.

        It's important to specify here that this function was created to handle the _same table_ between the `left_table` and the `right_table`, which are existing between different layers in the ADLS environment. Logically, it can be used for other purposes (it's generic enough); however, the intention was specifically for cleaning during the data pipeline processes.

    Params:
        left_table (psDataFrame):
            The DataFrame _from which_ you will be deleting the records.
        right_table (psDataFrame):
            The DataFrame _from which_ to check for existing records. If any matching `on_keys` are existing on both the `right_table` and the `left_table`, then those records will be deleted from the `left_table`.
        on_keys (Union[str, str_collection]):
            The matching keys between the two tables. These keys (aka columns) must be existing on both the `left_table` and the `right_table`.
        join_type (VALID_PYAPARK_JOIN_TYPES, optional):
            The type of join to use for this process. For the best performance, keep it as the default value.<br>
            Defaults to `#!py "left_anti"`.
        where_clause (Optional[str], optional):
            Any additional conditions to place on this join. Any records which **match** this condition will be **kept** on the `left_table`.<br>
            Defaults to `#!py None`.

    Returns:
        (psDataFrame):
            The `left_table` after it has had it's rows deleted and cleaned by the `right_table`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.cleaning import drop_matching_rows
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> left = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": [1, 1, 1, 1],
        ...             "d": ["2", "2", "2", "2"],
        ...             "n": ["a", "b", "c", "d"],
        ...         }
        ...     )
        ... )
        ... right = left.where("a in ("1", "2")")
        >>>
        >>> # Check
        ```{.py .python linenums="1" title="Check"}
        >>> left.show()
        >>> right.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 1 | a | 1 | 2 | a |
        | 2 | b | 1 | 2 | b |
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 1 | a | 1 | 2 | a |
        | 2 | b | 1 | 2 | b |
        +---+---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Single column"}
        >>> drop_matching_rows(
        ...     left_table=left,
        ...     right_table=right,
        ...     on_keys=["a"],
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully removed the records from the `left_table` which are existing on the `right_table`."
        </div>

        ```{.py .python linenums="1" title="Example 2: Single column as string"}
        >>> left.transform(
        ...     drop_matching_rows,
        ...     right_table=right,
        ...     on_keys="a",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully removed the records from the `left_table` which are existing on the `right_table`."
        </div>

        ```{.py .python linenums="1" title="Example 3: Multiple key columns"}
        >>> drop_matching_rows(
        ...     left_table=left,
        ...     right_table=right,
        ...     on_keys=["a", "b"],
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully removed the records from the `left_table` which are existing on the `right_table`."
        </div>

        ```{.py .python linenums="1" title="Example 4: Including `where` clause"}
        >>> drop_matching_rows(
        ...     left_table=left,
        ...     right_table=right,
        ...     on_keys=["a"],
        ...     where_clause="n <> 'd'",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 3 | c | 1 | 2 | c |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully removed the records from the `left_table` which are existing on the `right_table` and matched the `where` clause."
        </div>

        ```{.py .python linenums="1" title="Example 5: Part of pipe"}
        >>> left.transform(
        ...     func=drop_matching_rows,
        ...     right_table=right,
        ...     on_keys=["a"],
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully removed the records from the `left_table` which are existing on the `right_table`."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid column"}
        >>> drop_matching_rows(
        ...     left_table=left,
        ...     right_table=right,
        ...     on_keys=["f"],
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ['f'] do not exist in the DataFrame.
        Try one of: ["a", "b", "c", "d", "n"]
        ```
        !!! failure "Conclusion: Columns do not exist."
        </div>

    ??? info "Notes"
        - The `on_keys` parameter can be a single string or a list of strings. This is to allow for multiple columns to be used as the matching keys.
        - The `where_clause` parameter is optional. If specified, then only the records which match the condition will be kept on the `left_table`. It is applied after the join. If not specified, then all records which are existing on the `right_table` will be removed from the `left_table`.

    ??? tip "See Also"
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
    """
    on_keys = [on_keys] if is_type(on_keys, str) else on_keys
    assert_columns_exists(left_table, on_keys, False)
    assert_columns_exists(right_table, on_keys, False)
    return (
        left_table.alias("left")
        .join(right_table.alias("right"), on=on_keys, how=join_type)
        .where("1=1" if where_clause is None else where_clause)
        .select([f"left.{col}" for col in left_table.columns])
    )
