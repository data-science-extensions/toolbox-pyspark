# ============================================================================ #
#                                                                              #
#     Title   : Scale                                                          #
#     Purpose : Rounding a column (or columns) to a given rounding accuracy.   #
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
    The `scale` module is used for rounding a column (or columns) to a given rounding accuracy.
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
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import assert_column_exists, assert_columns_exists


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = ["round_column", "round_columns"]


# ---------------------------------------------------------------------------- #
#  Constants                                                                ####
# ---------------------------------------------------------------------------- #


DEFAULT_DECIMAL_ACCURACY: int = 10
VALID_TYPES: str_list = ["float", "double", "decimal"]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Firstly                                                                  ####
# ---------------------------------------------------------------------------- #


@typechecked
def round_column(
    dataframe: psDataFrame,
    column: str,
    scale: int = DEFAULT_DECIMAL_ACCURACY,
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given `dataframe`, on a given `column` if the column data type is decimal (that is, one of: `#!py ["float", "double", "decimal"]`), then round that column to a `scale` accuracy at a given number of decimal places.

    ???+ abstract "Details"
        Realistically, under the hood, this function is super simple. It merely runs:
        ```{.py .python linenums="1" title="Python"}
        dataframe = dataframe.withColumn(colName=column, col=F.round(col=column, scale=scale))
        ```
        This function merely adds some additional validation, and is enabled to run in a pyspark `.transform()` method.
        For more info, see: [`pyspark.sql.DataFrame.transform`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.transform.html)

    Params:
        dataframe (psDataFrame):
            The `dataframe` to be transformed.
        column (str):
            The desired column to be rounded.
        scale (int, optional):
            The required level of rounding for the column.<br>
            If not provided explicitly, it will default to the global value `#!py DEFAULT_DECIMAL_ACCURACY`; which is `#!py 10`.<br>
            Defaults to `#!py DEFAULT_DECIMAL_ACCURACY`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        TypeError:
            If the given `column` is not one of the correct data types for rounding. It must be one of: `#!py ["float", "double", "decimal"]`.

    Returns:
        (psDataFrame):
            The transformed `dataframe` containing the column which has now been rounded.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession, functions as F, types as T
        >>> from toolbox_pyspark.io import read_from_path
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = (
        ...     spark
        ...     .createDataFrame(
        ...         pd.DataFrame(
        ...             {
        ...                 "a": range(20),
        ...                 "b": [f"1.{'0'*val}1" for val in range(20)],
        ...                 "c": [f"1.{'0'*val}6" for val in range(20)],
        ...             }
        ...         )
        ...     )
        ...     .withColumns(
        ...         {
        ...             "b": F.col("b").cast(T.DecimalType(21, 20)),
        ...             "c": F.col("c").cast(T.DecimalType(21, 20)),
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----------------------+----------------------+
        |a  |b                     |c                     |
        +---+----------------------+----------------------+
        |0  |1.10000000000000000000|1.60000000000000000000|
        |1  |1.01000000000000000000|1.06000000000000000000|
        |2  |1.00100000000000000000|1.00600000000000000000|
        |3  |1.00010000000000000000|1.00060000000000000000|
        |4  |1.00001000000000000000|1.00006000000000000000|
        |5  |1.00000100000000000000|1.00000600000000000000|
        |6  |1.00000010000000000000|1.00000060000000000000|
        |7  |1.00000001000000000000|1.00000006000000000000|
        |8  |1.00000000100000000000|1.00000000600000000000|
        |9  |1.00000000010000000000|1.00000000060000000000|
        |10 |1.00000000001000000000|1.00000000006000000000|
        |11 |1.00000000000100000000|1.00000000000600000000|
        |12 |1.00000000000010000000|1.00000000000060000000|
        |13 |1.00000000000001000000|1.00000000000006000000|
        |14 |1.00000000000000100000|1.00000000000000600000|
        |15 |1.00000000000000010000|1.00000000000000060000|
        |16 |1.00000000000000001000|1.00000000000000006000|
        |17 |1.00000000000000000100|1.00000000000000000600|
        |18 |1.00000000000000000010|1.00000000000000000060|
        |19 |1.00000000000000000001|1.00000000000000000006|
        +---+----------------------+----------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Round with defaults"}
        >>> round_column(df, "b").show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+------------+----------------------+
        |a  |b           |c                     |
        +---+------------+----------------------+
        |0  |1.1000000000|1.60000000000000000000|
        |1  |1.0100000000|1.06000000000000000000|
        |2  |1.0010000000|1.00600000000000000000|
        |3  |1.0001000000|1.00060000000000000000|
        |4  |1.0000100000|1.00006000000000000000|
        |5  |1.0000010000|1.00000600000000000000|
        |6  |1.0000001000|1.00000060000000000000|
        |7  |1.0000000100|1.00000006000000000000|
        |8  |1.0000000010|1.00000000600000000000|
        |9  |1.0000000001|1.00000000060000000000|
        |10 |1.0000000000|1.00000000006000000000|
        |11 |1.0000000000|1.00000000000600000000|
        |12 |1.0000000000|1.00000000000060000000|
        |13 |1.0000000000|1.00000000000006000000|
        |14 |1.0000000000|1.00000000000000600000|
        |15 |1.0000000000|1.00000000000000060000|
        |16 |1.0000000000|1.00000000000000006000|
        |17 |1.0000000000|1.00000000000000000600|
        |18 |1.0000000000|1.00000000000000000060|
        |19 |1.0000000000|1.00000000000000000006|
        +---+------------+----------------------+
        ```
        !!! success "Conclusion: Successfully rounded column `b`."
        </div>

        ```{.py .python linenums="1" title="Example 2: Round to custom number"}
        >>> round_column(df, "c", 5).show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----------------------+-------+
        |a  |b                     |c      |
        +---+----------------------+-------+
        |0  |1.10000000000000000000|1.60000|
        |1  |1.01000000000000000000|1.06000|
        |2  |1.00100000000000000000|1.00600|
        |3  |1.00010000000000000000|1.00060|
        |4  |1.00001000000000000000|1.00006|
        |5  |1.00000100000000000000|1.00001|
        |6  |1.00000010000000000000|1.00000|
        |7  |1.00000001000000000000|1.00000|
        |8  |1.00000000100000000000|1.00000|
        |9  |1.00000000010000000000|1.00000|
        |10 |1.00000000001000000000|1.00000|
        |11 |1.00000000000100000000|1.00000|
        |12 |1.00000000000010000000|1.00000|
        |13 |1.00000000000001000000|1.00000|
        |14 |1.00000000000000100000|1.00000|
        |15 |1.00000000000000010000|1.00000|
        |16 |1.00000000000000001000|1.00000|
        |17 |1.00000000000000000100|1.00000|
        |18 |1.00000000000000000010|1.00000|
        |19 |1.00000000000000000001|1.00000|
        +---+----------------------+-------+
        ```
        !!! success "Conclusion: Successfully rounded column `b` to 5 decimal points."
        </div>

        ```{.py .python linenums="1" title="Example 3: Raise error"}
        >>> round_column(df, "a").show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        TypeError: Column is not the correct type. Please check.
        For column 'a', the type is 'bigint'.
        In order to round it, it needs to be one of: '["float", "double", "decimal"]'.
        ```
        !!! failure "Conclusion: Cannot round a column `a`."
        </div>
    """
    assert_column_exists(dataframe, column)
    col_type: str = [typ.split("(")[0] for col, typ in dataframe.dtypes if col == column][0]
    if col_type not in VALID_TYPES:
        raise TypeError(
            f"Column is not the correct type. Please check.\n"
            f"For column '{column}', the type is '{col_type}'.\n"
            f"In order to round it, it needs to be one of: '{VALID_TYPES}'."
        )
    return dataframe.withColumn(colName=column, col=F.round(col=column, scale=scale))


@typechecked
def round_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = "all_float",
    scale: int = DEFAULT_DECIMAL_ACCURACY,
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given `dataframe`, on a set of `columns` if the column data type is decimal (that is, one of: `#!py ["float", "double", "decimal"]`), then round that column to a `scale` accuracy at a given number of decimal places.

    ???+ abstract "Details"
        Realistically, under the hood, this function is super simple. It merely runs:
        ```{.py .python linenums="1" title="Python"}
        dataframe = dataframe.withColumns({col: F.round(col, scale) for col in columns})
        ```
        This function merely adds some additional validation, and is enabled to run in a pyspark `.transform()` method.
        For more info, see: [`pyspark.sql.DataFrame.transform`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.transform.html)

    Params:
        dataframe (psDataFrame):
            The `dataframe` to be transformed.
        columns (Optional[Union[str, str_collection]], optional):
            The desired column to be rounded.<br>
            If no value is parsed, or is the value `#!py None`, or one of `#!py ["all", "all_float"]`, then it will default to all numeric decimal columns on the `dataframe`.<br>
            If the value is a `#!py str`, then it will be coerced to a single-element list, like: `#!py [columns]`.<br>
            Defaults to `#!py "all_float"`.
        scale (int, optional):
            The required level of rounding for the column.<br>
            If not provided explicitly, it will default to the global value `#!py DEFAULT_DECIMAL_ACCURACY`; which is `#!py 10`.<br>
            Defaults to `#!py DEFAULT_DECIMAL_ACCURACY`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        TypeError:
            If any of the given `columns` are not one of the correct data types for rounding. They must be one of: `#!py ["float", "double", "decimal"]`.

    Returns:
        (psDataFrame):
            The transformed `dataframe` containing the column which has now been rounded.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession, functions as F, types as T
        >>> from toolbox_pyspark.io import read_from_path
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = (
        ...     spark
        ...     .createDataFrame(
        ...         pd.DataFrame(
        ...             {
        ...                 "a": range(20),
        ...                 "b": [f"1.{'0'*val}1" for val in range(20)],
        ...                 "c": [f"1.{'0'*val}6" for val in range(20)],
        ...             }
        ...         )
        ...     )
        ...     .withColumns(
        ...         {
        ...             "b": F.col("b").cast(T.DecimalType(21, 20)),
        ...             "c": F.col("c").cast(T.DecimalType(21, 20)),
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----------------------+----------------------+
        |a  |b                     |c                     |
        +---+----------------------+----------------------+
        |0  |1.10000000000000000000|1.60000000000000000000|
        |1  |1.01000000000000000000|1.06000000000000000000|
        |2  |1.00100000000000000000|1.00600000000000000000|
        |3  |1.00010000000000000000|1.00060000000000000000|
        |4  |1.00001000000000000000|1.00006000000000000000|
        |5  |1.00000100000000000000|1.00000600000000000000|
        |6  |1.00000010000000000000|1.00000060000000000000|
        |7  |1.00000001000000000000|1.00000006000000000000|
        |8  |1.00000000100000000000|1.00000000600000000000|
        |9  |1.00000000010000000000|1.00000000060000000000|
        |10 |1.00000000001000000000|1.00000000006000000000|
        |11 |1.00000000000100000000|1.00000000000600000000|
        |12 |1.00000000000010000000|1.00000000000060000000|
        |13 |1.00000000000001000000|1.00000000000006000000|
        |14 |1.00000000000000100000|1.00000000000000600000|
        |15 |1.00000000000000010000|1.00000000000000060000|
        |16 |1.00000000000000001000|1.00000000000000006000|
        |17 |1.00000000000000000100|1.00000000000000000600|
        |18 |1.00000000000000000010|1.00000000000000000060|
        |19 |1.00000000000000000001|1.00000000000000000006|
        +---+----------------------+----------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Round with defaults"}
        >>> round_columns(df).show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+------------+------------+
        |  a|           b|           c|
        +---+------------+------------+
        |  0|1.1000000000|1.6000000000|
        |  1|1.0100000000|1.0600000000|
        |  2|1.0010000000|1.0060000000|
        |  3|1.0001000000|1.0006000000|
        |  4|1.0000100000|1.0000600000|
        |  5|1.0000010000|1.0000060000|
        |  6|1.0000001000|1.0000006000|
        |  7|1.0000000100|1.0000000600|
        |  8|1.0000000010|1.0000000060|
        |  9|1.0000000001|1.0000000006|
        | 10|1.0000000000|1.0000000001|
        | 11|1.0000000000|1.0000000000|
        | 12|1.0000000000|1.0000000000|
        | 13|1.0000000000|1.0000000000|
        | 14|1.0000000000|1.0000000000|
        | 15|1.0000000000|1.0000000000|
        | 16|1.0000000000|1.0000000000|
        | 17|1.0000000000|1.0000000000|
        | 18|1.0000000000|1.0000000000|
        | 19|1.0000000000|1.0000000000|
        +---+------------+------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 2: Round to custom number"}
        >>> round_columns(df, "c", 5).show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+----------------------+-------+
        |a  |b                     |c      |
        +---+----------------------+-------+
        |0  |1.10000000000000000000|1.60000|
        |1  |1.01000000000000000000|1.06000|
        |2  |1.00100000000000000000|1.00600|
        |3  |1.00010000000000000000|1.00060|
        |4  |1.00001000000000000000|1.00006|
        |5  |1.00000100000000000000|1.00001|
        |6  |1.00000010000000000000|1.00000|
        |7  |1.00000001000000000000|1.00000|
        |8  |1.00000000100000000000|1.00000|
        |9  |1.00000000010000000000|1.00000|
        |10 |1.00000000001000000000|1.00000|
        |11 |1.00000000000100000000|1.00000|
        |12 |1.00000000000010000000|1.00000|
        |13 |1.00000000000001000000|1.00000|
        |14 |1.00000000000000100000|1.00000|
        |15 |1.00000000000000010000|1.00000|
        |16 |1.00000000000000001000|1.00000|
        |17 |1.00000000000000000100|1.00000|
        |18 |1.00000000000000000010|1.00000|
        |19 |1.00000000000000000001|1.00000|
        +---+----------------------+-------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 3: Raise error"}
        >>> round_columns(df, ["a", "b"]).show(truncate=False)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        TypeError: Columns are not the correct types. Please check.
        These columns are invalid: '[("a", "bigint")]'.
        In order to round them, they need to be one of: '["float", "double", "decimal"]'.
        ```
        </div>
    """
    if columns is None or columns in ["all", "all_float"]:
        columns = [col for col, typ in dataframe.dtypes if typ.split("(")[0] in VALID_TYPES]
    elif is_type(columns, str):
        columns = [columns]
    assert_columns_exists(dataframe, columns)
    invalid_cols: list[tuple[str, str]] = [
        (col, typ.split("(")[0])
        for col, typ in dataframe.dtypes
        if col in columns and typ.split("(")[0] not in VALID_TYPES
    ]
    if len(invalid_cols) > 0:
        raise TypeError(
            f"Columns are not the correct types. Please check.\n"
            f"These columns are invalid: '{invalid_cols}'.\n"
            f"In order to round them, they need to be one of: '{VALID_TYPES}'."
        )
    return dataframe.withColumns({col: F.round(col, scale) for col in columns})
