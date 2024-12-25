# ============================================================================ #
#                                                                              #
#     Title   : Duplication                                                    #
#     Purpose : Duplicate from an existing `dataframe`, or union multiple      #
#               `dataframe`'s together.                                        #
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
    The `duplication` module is used for duplicating data from an existing `dataframe`, or unioning multiple `dataframe`'s together.
"""


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
from pyspark.sql import (
    DataFrame as psDataFrame,
    functions as F,
)
from toolbox_python.collection_types import str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.cleaning import get_column_values


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "duplicate_union_dataframe",
    "union_all",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Firstly                                                                  ####
# ---------------------------------------------------------------------------- #


@typechecked
def duplicate_union_dataframe(
    dataframe: psDataFrame,
    by_list: str_list,
    new_column_name: str,
) -> psDataFrame:
    """
    !!! note "Summary"
        The purpose here is to take a given table and duplicate it entirely multiple times from values in a list, then union them all together.

    ???+ abstract "Details"
        There are sometimes instances where we need to duplicate an entire table multiple times, with no change to the underlying data. Sometimes this is to maintain the structure of the data, but duplicate it to match a different table structure. This function is designed to do just that.<br>
        The `dataframe` is the table to be duplicated, the `by_list` is the list of values to loop over, and the `new_column_name` is the new column to hold the loop values.

    Params:
        dataframe (psDataFrame):
            The table to be duplicated.
        by_list (str_list):
            The list to loop over.
        new_column_name (str):
            The new column to hold the loop values.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If any given value in the `by_list` list is not a string.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.duplication import duplicate_union_dataframe
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
        ...             "c": ["x", "x", "x", "x"],
        ...             "d": [2, 2, 2, 2],
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
        | 1 | a | x | 2 |
        | 2 | b | x | 2 |
        | 3 | c | x | 2 |
        | 4 | d | x | 2 |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Column missing"}
        >>> duplicate_union_dataframe(
        ...     dataframe=df,
        ...     by_list=["x", "y", "z"],
        ...     new_column_name="n",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 1 | a | x | 2 | x |
        | 2 | b | x | 2 | x |
        | 3 | c | x | 2 | x |
        | 4 | d | x | 2 | x |
        | 1 | a | x | 2 | y |
        | 2 | b | x | 2 | y |
        | 3 | c | x | 2 | y |
        | 4 | d | x | 2 | y |
        | 1 | a | x | 2 | z |
        | 2 | b | x | 2 | z |
        | 3 | c | x | 2 | z |
        | 4 | d | x | 2 | z |
        +---+---+---+---+---+
        ```
        !!! success "Conclusion: Successfully duplicated data frame multiple times."
        </div>

        ```{.py .python linenums="1" title="Example 2: Column existing"}
        >>> duplicate_union_dataframe(
        ...     dataframe=df,
        ...     by_list=["x", "y", "z"],
        ...     new_column_name="c",
        ... ).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 1 | a | x | 2 |
        | 2 | b | x | 2 |
        | 3 | c | x | 2 |
        | 4 | d | x | 2 |
        | 1 | a | y | 2 |
        | 2 | b | y | 2 |
        | 3 | c | y | 2 |
        | 4 | d | y | 2 |
        | 1 | a | z | 2 |
        | 2 | b | z | 2 |
        | 3 | c | z | 2 |
        | 4 | d | z | 2 |
        +---+---+---+---+
        ```
        !!! success "Conclusion: Successfully duplicated data frame multiple times."
        </div>

    ??? info "Notes"
        - How the `union` is performed:
            - Currently this function uses the `loop` and `append` method.
            - It was written this way because it's a lot easier and more logical for humans to understand.
            - However, there's probably a more computationally efficient method for doing this by using SQL Joins.
            - More specifically, for creating a CARTESIAN PRODUCT (aka a 'Cross-Join') over the data set.
            - This is probably one of the only times EVER that a developer would _want_ to create a cartesian product.
            - All other times a cartesian product is to be avoided at all costs...
        - Whether or not the column `new_column_name` exists or not on the `dataframe`:
            - The process is a little different for if the `new_column_name` is existing or not...
            - If it is existing, we need to:
                - Extract the `#!sql distinct` values from that column,
                - Create a duplicate copy of the raw table,
                - Loop through all values in `by_list`,
                - Check if that `value` from `by_list` is already existing in the extracted values from the `new_column_name` column,
                - If it is already existing, proceed to next iteration,
                - If it is not existing, take the raw table, update `new_column_name` to be the `value` from that iteration of `by_list`, then `#!sql union` that to the copy of the raw table,
                - Continue to iterate through all values in `by_list` until they're all `#!sql union`'ed together.
            - If it is not existing, we need to:
                - Add a new column to `dataframe` that has the name from `new_column_name`, and a single literal value from the zero'th index of the `by_list`,
                - Then to go through the same process as if the column were existing.
            - Having now achieved this, the final output `dataframe` will now have all the updated duplicate values that we require.

    ???+ warning "Warning"
        Obviously, it's easy to see how this function will blow out the size of a table to tremendious sizes. So be careful!
    """

    def _self_union_dataframe_with_column_existing(
        dataframe: psDataFrame,
        by_list: str_list,
        new_column_name: str,
    ) -> psDataFrame:
        values_in_col: list = get_column_values(
            dataframe=dataframe,
            column=new_column_name,
            distinct=True,
            return_type="flat_list",
        )
        new_df: psDataFrame = dataframe
        for value in by_list:
            if value in values_in_col:  # type: ignore
                continue
            new_df = new_df.unionAll(dataframe.withColumn(new_column_name, F.lit(value)))
        return new_df

    def _self_union_dataframe_with_column_missing(
        dataframe: psDataFrame,
        by_list: str_list,
        new_column_name: str,
    ) -> psDataFrame:
        new_df = dataframe.withColumn(new_column_name, F.lit(by_list[0]))
        return _self_union_dataframe_with_column_existing(
            dataframe=new_df,
            by_list=by_list,
            new_column_name=new_column_name,
        )

    if new_column_name in dataframe.columns:
        return _self_union_dataframe_with_column_existing(
            dataframe=dataframe,
            by_list=by_list,
            new_column_name=new_column_name,
        )
    else:
        return _self_union_dataframe_with_column_missing(
            dataframe=dataframe,
            by_list=by_list,
            new_column_name=new_column_name,
        )


@typechecked
def union_all(dfs: list[psDataFrame]) -> psDataFrame:
    """
    !!! note "Summary"
        Take a list of `dataframes`, and union them all together.

    ???+ abstract "Details"
        If any columns are missing or added in any of the `dataframes` within `dfs`, then they will be automatically handled with the `allowMissingColumns` parameter, and any of the other `dataframes` will simply contain `#!sql null` values for those columns which they are missing.

    Params:
        dfs (list[psDataFrame]):
            The list of `dataframe`'s to union together.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (psDataFrame):
            A single `dataframe` containing a union of all the `dataframe`s.

    ???+ example "Examples"
        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.duplication import duplicate_union_dataframe
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df1 = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": [1, 1, 1, 1],
        ...             "d": [2, 2, 2, 2],
        ...         })
        ... )
        >>> df2 = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a': [1, 2, 3, 4],
        ...             "b': ["a", "b", "c", "d"],
        ...             "c': [1, 1, 1, 1],
        ...     })
        ... )
        >>> df3 = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a': [1, 2, 3, 4],
        ...             "b': ["a", "b", "c", "d"],
        ...             "c': [1, 1, 1, 1],
        ...             "e': [3, 3, 3, 3],
        ...     })
        ... )
        >>> dfs = [df1, df2, df3]
        >>>
        >>> # Check
        >>> for df in dfs:
        ...     df.show()
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
        ```{.txt .text title="Terminal"}
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | 1 |
        | 2 | b | 1 |
        | 3 | c | 1 |
        | 4 | d | 1 |
        +---+---+---+
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+---+
        | a | b | c | e |
        +---+---+---+---+
        | 1 | a | 1 | 3 |
        | 2 | b | 1 | 3 |
        | 3 | c | 1 | 3 |
        | 4 | d | 1 | 3 |
        +---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Basic usage"}
        >>> union_all(dfs).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+------+------+
        | a | b | c |    d |    e |
        +---+---+---+------+------+
        | 1 | a | 1 |    2 | null |
        | 2 | b | 1 |    2 | null |
        | 3 | c | 1 |    2 | null |
        | 4 | d | 1 |    2 | null |
        | 1 | a | 1 | null | null |
        | 2 | b | 1 | null | null |
        | 3 | c | 1 | null | null |
        | 4 | d | 1 | null | null |
        | 1 | a | 1 | null |    3 |
        | 2 | b | 1 | null |    3 |
        | 3 | c | 1 | null |    3 |
        | 4 | d | 1 | null |    3 |
        +---+---+---+------+------+
        ```
        !!! success "Conclusion: Successfully unioned all data frames together."
        </div>
    """
    if len(dfs) > 1:
        return dfs[0].unionByName(union_all(dfs[1:]), allowMissingColumns=True)
    else:
        return dfs[0]
