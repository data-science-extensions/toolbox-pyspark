# ============================================================================ #
#                                                                              #
#     Title: Info                                                              #
#     Purpose: ...                                                             #
#                                                                              #
# ============================================================================ #


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Imports                                                                 ####
## --------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from typing import Optional, Union

# ## Python Third Party Imports ----
from numpy import ndarray as npArray
from pandas import DataFrame as pdDataFrame
from pyspark.sql import DataFrame as psDataFrame, types as T
from toolbox_python.collection_types import str_list, str_tuple
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.cleaning import convert_dataframe
from toolbox_pyspark.constants import (
    LITERAL_LIST_OBJECT_NAMES,
    LITERAL_NUMPY_ARRAY_NAMES,
    LITERAL_PANDAS_DATAFRAME_NAMES,
    LITERAL_PYSPARK_DATAFRAME_NAMES,
)


## --------------------------------------------------------------------------- #
##  Exports                                                                 ####
## --------------------------------------------------------------------------- #


__all__: str_list = ["get_distinct_values"]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Main Section                                                          ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  `get_*()` functions                                                     ####
## --------------------------------------------------------------------------- #


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

    ??? tip "See Also"
        - [`toolbox_pyspark.cleaning.convert_dataframe()`][toolbox_pyspark.cleaning.convert_dataframe]
        - [`toolbox_pyspark.constants`][toolbox_pyspark.constants]
    """
    df: psDataFrame = dataframe.select(column).filter(
        f"{column} is not null and {column} <> ''"
    )
    df = df.distinct() if distinct else df
    return convert_dataframe(dataframe=df, return_type=return_type)


def get_distinct_values(dataframe: psDataFrame, column: str) -> str_tuple:
    rows: list[T.Row] = dataframe.select(column).distinct().collect()
    return tuple(row[column] for row in rows)
