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


__all__: str_list = ["get_distinct_values", "extract_column_values"]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Main Section                                                          ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  `get_*()` functions                                                     ####
## --------------------------------------------------------------------------- #


@typechecked
def extract_column_values(
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
        Retrieve the values from a specified column in a `pyspark` dataframe.

    Params:
        dataframe (psDataFrame):
            The DataFrame to retrieve the column values from.
        column (str):
            The column to retrieve the values from.
        distinct (bool, optional):
            Whether to retrieve only distinct values.<br>
            Defaults to `#!py True`.
        return_type (Union[str, LITERAL_PYSPARK_DATAFRAME_NAMES, LITERAL_PANDAS_DATAFRAME_NAMES, LITERAL_NUMPY_ARRAY_NAMES, LITERAL_LIST_OBJECT_NAMES], optional):
            The type of object to return.<br>
            Defaults to `#!py "pd"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValueError:
            If the `return_type` is not one of the valid options.
        ColumnDoesNotExistError:
            If the `#!py column` does not exist within `#!py dataframe.columns`.

    Returns:
        (Optional[Union[psDataFrame, pdDataFrame, npArray, list]]):
            The values from the specified column in the specified return type.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.info import get_column_values
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
        ...             "c": [1, 1, 1, 1],
        ...             "d": ["2", "2", "2", "2"],
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
        | 1 | a | 1 | 2 |
        | 2 | b | 1 | 2 |
        | 3 | c | 1 | 2 |
        | 4 | d | 1 | 2 |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Retrieve distinct values as pandas DataFrame"}
        >>> result = get_column_values(df, "b", distinct=True, return_type="pd")
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
           b
        0  a
        1  b
        2  c
        3  d
        ```
        !!! success "Conclusion: Successfully retrieved distinct values as pandas DataFrame."
        </div>

        ```{.py .python linenums="1" title="Example 2: Retrieve all values as list"}
        >>> result = get_column_values(df, "c", distinct=False, return_type="list")
        >>> print(result)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ['1', '1', '1', '1']
        ```
        !!! success "Conclusion: Successfully retrieved all values as list."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid return type"}
        >>> result = get_column_values(df, "b", distinct=True, return_type="invalid")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ValueError: Invalid return type: invalid
        ```
        !!! failure "Conclusion: Failed to retrieve values due to invalid return type."
        </div>

    ??? tip "See Also"
        - [`get_distinct_values`][toolbox_pyspark.info.get_distinct_values]
    """
    assert_column_exists(dataframe, column)
    if return_type not in ["ps", "pd", "np", "list"]:
        raise ValueError(f"Invalid return type: {return_type}")

    if distinct:
        dataframe = dataframe.select(column).distinct()

    if return_type == "ps":
        return dataframe
    elif return_type == "pd":
        return dataframe.toPandas()
    elif return_type == "np":
        return dataframe.select(column).toPandas().to_numpy()
    elif return_type == "list":
        return dataframe.select(column).toPandas()[column].tolist()
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
