# ============================================================================ #
#                                                                              #
#     Title   : Keys                                                           #
#     Purpose : Creating new columns to act as keys (primary and foreign),     #
#               to be used for joins with other tables, or to create           #
#               relationships within downstream applications, like PowerBI.    #
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
    The `keys` module is used for creating new columns to act as keys (primary and foreign), to be used for joins with other tables, or to create relationships within downstream applications, like PowerBI.
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
from toolbox_pyspark.checks import assert_columns_exists


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = ["add_keys_from_columns", "add_key_from_columns"]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Add Keys                                                                 ####
# ---------------------------------------------------------------------------- #


@typechecked
def add_key_from_columns(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
    join_character: Optional[str] = "_",
    key_name: Optional[str] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Using a list of column names, add a new column which is a combination of all of them.

    ???+ abstract "Details"
        This is a combine key, and is especially important because PowerBI cannot handle joins on multiple columns.

    Params:
        dataframe (psDataFrame):
            The table to be updated.
        columns (Union[str, str_collection]):
            The columns to be combined.<br>
            If `columns` is a `#!py str`, then it will be coerced to a single-element list: `#!py [columns]`.
        join_character (Optional[str], optional):
            The character to use to combine the columns together.<br>
            Defaults to `#!py "_"`.
        key_name (Optional[str], optional):
            The name of the column to be given to the key.
            If not provided, it will form as the capitalised string of all the other column names, prefixed with `key_`.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated `dataframe`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import get_column_types
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

        ```{.py .python linenums="1" title="Example 1: Basic usage"}
        >>> new_df = add_key_from_columns(df, ["a", "b"])
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---------+
        | a | b | c | d | key_A_B |
        +---+---+---+---+---------+
        | 1 | a | 1 | 2 | 1_a     |
        | 2 | b | 1 | 2 | 2_b     |
        | 3 | c | 1 | 2 | 3_c     |
        | 4 | d | 1 | 2 | 4_d     |
        +---+---+---+---+---------+
        ```
        !!! success "Conclusion: Successfully added new key column to DataFrame."
        </div>

        ```{.py .python linenums="1" title="Example 2: Single column"}
        >>> new_df = add_key_from_columns(df, "a")
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+-------+
        | a | b | c | d | key_A |
        +---+---+---+---+-------+
        | 1 | a | 1 | 2 | 1     |
        | 2 | b | 1 | 2 | 2     |
        | 3 | c | 1 | 2 | 3     |
        | 4 | d | 1 | 2 | 4     |
        +---+---+---+---+-------+
        ```
        !!! success "Conclusion: Successfully added new key column to DataFrame."
        </div>

        ```{.py .python linenums="1" title="Example 3: New name"}
        >>> new_df = add_key_from_columns(df, ["a", "b"], "new_key")
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---------+
        | a | b | c | d | new_key |
        +---+---+---+---+---------+
        | 1 | a | 1 | 2 | 1_a     |
        | 2 | b | 1 | 2 | 2_b     |
        | 3 | c | 1 | 2 | 3_c     |
        | 4 | d | 1 | 2 | 4_d     |
        +---+---+---+---+---------+
        ```
        !!! success "Conclusion: Successfully added new key column to DataFrame."
        </div>

        ```{.py .python linenums="1" title="Example 4: Raise error"}
        >>> new_df = add_key_from_columns(df, ["a", "x"])
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Attribute Error: Columns ["x"] do not exist in "dataframe". Try one of: ["a", "b", "c", "d"].
        ```
        !!! failure "Conclusion: Invalid column selection."
        </div>
    """
    columns = [columns] if is_type(columns, str) else columns
    assert_columns_exists(dataframe, columns)
    join_character = join_character or ""
    key_name = key_name or f"key_{'_'.join([col.upper() for col in columns])}"
    return dataframe.withColumn(
        key_name,
        F.concat_ws(join_character, *columns),
    )


@typechecked
def add_keys_from_columns(
    dataframe: psDataFrame,
    collection_of_columns: Union[
        tuple[Union[str, str_collection], ...],
        list[Union[str, str_collection]],
        dict[str, Union[str, str_collection]],
    ],
    join_character: Optional[str] = "_",
) -> psDataFrame:
    """
    !!! note "Summary"
        Add multiple new keys, each of which are collections of other columns.

    ???+ abstract "Details"
        There are a few reasons why this functionality would be needed:

        1. When you wanted to create a new single column to act as a combine key, derived from multiple other columns.
        1. When you're interacting with PowerBI, it will only allow you to create relationships on one single column, not a combination of multiple columns.
        1. When you're joining multiple tables together, each of them join on a different combination of different columns, and you want to make your `pyspark` joins cleaner, instead of using `#!py list`'s of multiple `#!py F.col(...)` equality checks.

    Params:
        dataframe (psDataFrame):
            The table to be updated.
        collection_of_columns (Union[tuple[Union[str, str_collection], ...], [Union[str, str_collection]], dict[str, Union[str, str_collection]]]):
            The collection of columns to be combined together.<br>
            If it is a `#!py list` of `#!py list`'s of `#!py str`'s (or similar), then the key name will be derived from a concatenation of the original columns names.<br>
            If it's a `#!py dict` where the values are a `#!py list` of `#!py str`'s (or similar), then the column name for the new key is taken from the key of the dictionary.
        join_character (Optional[str], optional):
            The character to use to combine the columns together.<br>
            Defaults to `#!py "_"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated `dataframe`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import get_column_types
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

        ```{.py .python linenums="1" title="Example 1: Basic usage"}
        >>> new_df = add_keys_from_columns(df, [["a", "b"], ["b", "c"]])
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---------+---------+
        | a | b | c | d | key_A_B | key_B_C |
        +---+---+---+---+---------+---------+
        | 1 | a | 1 | 2 | 1_a     | a_1     |
        | 2 | b | 1 | 2 | 2_b     | b_1     |
        | 3 | c | 1 | 2 | 3_c     | c_1     |
        | 4 | d | 1 | 2 | 4_d     | d_1     |
        +---+---+---+---+---------+---------+
        ```
        !!! success "Conclusion: Successfully added two new key columns to DataFrame."
        </div>

        ```{.py .python linenums="1" title="Example 2: Created from dict"}
        >>> new_df = add_keys_from_columns(df, {"first": ["a", "b"], "second": ["b", "c"]])
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+-------+--------+
        | a | b | c | d | first | second |
        +---+---+---+---+-------+--------+
        | 1 | a | 1 | 2 | 1_a   | a_1    |
        | 2 | b | 1 | 2 | 2_b   | b_1    |
        | 3 | c | 1 | 2 | 3_c   | c_1    |
        | 4 | d | 1 | 2 | 4_d   | d_1    |
        +---+---+---+---+-------+--------+
        ```
        !!! success "Conclusion: Successfully added two new key columns to DataFrame."
        </div>
    """
    join_character = join_character or ""
    if is_type(collection_of_columns, dict):
        for key_name, columns in collection_of_columns.items():
            dataframe = add_key_from_columns(dataframe, columns, join_character, key_name)
    elif is_type(collection_of_columns, (tuple, list)):
        for columns in collection_of_columns:
            dataframe = add_key_from_columns(dataframe, columns, join_character)
    return dataframe
