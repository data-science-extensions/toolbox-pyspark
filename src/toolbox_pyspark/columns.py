# ============================================================================ #
#                                                                              #
#     Title   : Dataframe Cleaning                                             #
#     Purpose : Fetch columns from a given DataFrame using convenient syntax.  #
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
    The `columns` module is used to fetch columns from a given DataFrame using convenient syntax.
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
from typing import Literal, Optional, Union

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import (
    assert_columns_exists,
    warn_columns_missing,
)


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "get_columns",
    "get_columns_by_likeness",
    "rename_columns",
    "reorder_columns",
    "delete_columns",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Selecting                                                                ####
# ---------------------------------------------------------------------------- #


@typechecked
def get_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
) -> str_list:
    """
    !!! note "Summary"
        Get a list of column names from a DataFrame based on optional filter criteria.

    Params:
        dataframe (psDataFrame):
            The DataFrame from which to retrieve column names.
        columns (Optional[Union[str, str_collection]], optional):
            Optional filter criteria for selecting columns.<br>
            If a string is provided, it can be one of the following options:

            | Value | Description |
            |-------|-------------|
            | `#!py "all"` | Return all columns in the DataFrame.
            | `#!py "all_str"` | Return columns of string type.
            | `#!py "all_int"` | Return columns of integer type.
            | `#!py "all_numeric"` | Return columns of numeric types (integers and floats).
            | `#!py "all_datetime"` or `#!py "all_timestamp"` | Return columns of datetime or timestamp type.
            | `#!py "all_date"` | Return columns of date type.
            | Any other string | Return columns matching the provided exact column name.

            If a list or tuple of column names is provided, return only those columns.<br>
            Defaults to `#!py None` (which returns all columns).

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (str_list):
            The selected column names from the DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> from pprint import pprint
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession, functions as F
        >>> from toolbox_pyspark.columns import get_columns
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
        ...                 "a": (0, 1, 2, 3),
        ...                 "b": ["a", "b", "c", "d"],
        ...             }
        ...         )
        ...     )
        ...     .withColumns(
        ...         {
        ...             "c": F.lit("1").cast("int"),
        ...             "d": F.lit("2").cast("string"),
        ...             "e": F.lit("1.1").cast("float"),
        ...             "f": F.lit("1.2").cast("double"),
        ...             "g": F.lit("2022-01-01").cast("date"),
        ...             "h": F.lit("2022-02-01 01:00:00").cast("timestamp"),
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        >>> print(df.dtypes)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+-----+-----+------------+---------------------+
        | a | b | c | d |   e |   f |          g |                   h |
        +---+---+---+---+-----+-----+------------+---------------------+
        | 0 | a | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 1 | b | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 2 | c | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 3 | d | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        +---+---+---+---+-----+-----+------------+---------------------+
        ```
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "int"),
            ("d", "string"),
            ("e", "float"),
            ("f", "double"),
            ("g", "date"),
            ("h", "timestamp"),
        ]
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default params"}
        >>> print(get_columns(df).columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a", "b", "c", "d", "e", "f", "g", "h"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 2: Specific columns"}
        >>> print(get_columns(df, ["a", "b", "c"]).columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a", "b", "c"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 3: Single column as list"}
        >>> print(get_columns(df, ["a"]).columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 4: Single column as string"}
        >>> print(get_columns(df, "a").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 5: All columns"}
        >>> print(get_columns(df, "all").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a", "b", "c", "d", "e", "f", "g", "h"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 6: All str"}
        >>> print(get_columns(df, "all_str").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["b", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 7: All int"}
        >>> print(get_columns(df, "all int").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["c"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 8: All float"}
        >>> print(get_columns(df, "all_decimal").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["e", "f"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 9: All numeric"}
        >>> print(get_columns(df, "all_numeric").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["c", "e", "f"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 10: All date"}
        >>> print(get_columns(df, "all_date").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["g"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 11: All datetime"}
        >>> print(get_columns(df, "all_datetime").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["h"]
        ```
        !!! success "Conclusion: Success."
        </div>
    """
    if columns is None:
        return dataframe.columns
    elif is_type(columns, str):
        if "all" in columns:
            if "str" in columns:
                return [col for col, typ in dataframe.dtypes if typ in ("str", "string")]
            elif "int" in columns:
                return [col for col, typ in dataframe.dtypes if typ in ("int", "integer")]
            elif "numeric" in columns:
                return [
                    col
                    for col, typ in dataframe.dtypes
                    if typ in ("int", "integer", "float", "double", "long") or "decimal" in typ
                ]
            elif "float" in columns or "double" in columns or "decimal" in columns:
                return [
                    col
                    for col, typ in dataframe.dtypes
                    if typ in ("float", "double", "long") or "decimal" in typ
                ]
            elif "datetime" in columns or "timestamp" in columns:
                return [
                    col for col, typ in dataframe.dtypes if typ in ("datetime", "timestamp")
                ]
            elif "date" in columns:
                return [col for col, typ in dataframe.dtypes if typ in ["date"]]
            else:
                return dataframe.columns
        else:
            return [columns]
    else:
        return list(columns)


@typechecked
def get_columns_by_likeness(
    dataframe: psDataFrame,
    starts_with: Optional[str] = None,
    contains: Optional[str] = None,
    ends_with: Optional[str] = None,
    match_case: bool = False,
    operator: Literal["and", "or", "and not", "or not"] = "and",
) -> str_list:
    """
    !!! note "Summary"
        Extract the column names from a given `dataframe` based on text that the column name contains.

    ???+ abstract "Details"
        You can use any combination of `startswith`, `contains`, and `endswith`. Under the hood, these will be implemented with a number of internal `#!py lambda` functions to determine matches.

        The `operator` parameter determines how the conditions (`starts_with`, `contains`, `ends_with`) are combined:

        | Value | Description |
        |-------|-------------|
        | `"and"` | All conditions must be true.
        | `"or"` | At least one condition must be true.
        | `"and not"` | The first condition must be true and the second condition must be false.
        | `"or not"` | At least one condition must be true, but not all.

    Params:
        dataframe (psDataFrame):
            The `dataframe` from which to extract the column names.
        starts_with (Optional[str], optional):
            Extract any columns that starts with this `#!py str`.<br>
            Determined by using the `#!py str.startswith()` method.<br>
            Defaults to `#!py None`.
        contains (Optional[str], optional):
            Extract any columns that contains this `#!py str` anywhere within it.<br>
            Determined by using the `#!py in` keyword.<br>
            Defaults to `#!py None`.
        ends_with (Optional[str], optional):
            Extract any columns that ends with this `#!py str`.<br>
            Determined by using the `#!py str.endswith()` method.<br>
            Defaults to `#!py None`.
        match_case (bool, optional):
            If you want to ensure an exact match for the columns, set this to `#!py True`, else if you want to match the exact case for the columns, set this to `#!py False`.<br>
            Defaults to `#!py False`.
        operator (Literal["and", "or", "and not", "or not"], optional):
            The logical operator to place between the functions.<br>
            Only used when there are multiple values parsed to the parameters: `#!py starts_with`, `#!py contains`: `#!py ends_with`.<br>
            Defaults to `#!py and`.

    Returns:
        (str_list):
            The list of columns which match the criteria specified.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.columns import get_columns_by_likeness
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> values = list(range(1, 6))
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "aaa": values,
        ...             "aab": values,
        ...             "aac": values,
        ...             "afa": values,
        ...             "afb": values,
        ...             "afc": values,
        ...             "bac": values,
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-----+-----+-----+-----+-----+-----+-----+
        | aaa | aab | aac | afa | afb | afc | bac |
        +-----+-----+-----+-----+-----+-----+-----+
        |   1 |   1 |   1 |   1 |   1 |   1 |   1 |
        |   2 |   2 |   2 |   2 |   2 |   2 |   2 |
        |   3 |   3 |   3 |   3 |   3 |   3 |   3 |
        |   4 |   4 |   4 |   4 |   4 |   4 |   4 |
        |   5 |   5 |   5 |   5 |   5 |   5 |   5 |
        +-----+-----+-----+-----+-----+-----+-----+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Starts With"}
        >>> print(get_columns_by_likeness(df, starts_with="a"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aaa", "aab", "aac", "afa", "afb", "afc"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 2: Contains"}
        >>> print(get_columns_by_likeness(df, contains="f"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["afa", "afb", "afc"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 3: Ends With"}
        >>> print(get_columns_by_likeness(df, ends_with="c"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aac", "afc", "bac"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 4: Starts With and Contains"}
        >>> print(get_columns_by_likeness(df, starts_with="a", contains="c"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aac", "afc"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 5: Starts With and Ends With"}
        >>> print(get_columns_by_likeness(df, starts_with="a", ends_with="b"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aab", "afb"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 6: Contains and Ends With"}
        >>> print(get_columns_by_likeness(df, contains="f", ends_with="b"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["afb"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 7: Starts With and Contains and Ends With"}
        >>> print(get_columns_by_likeness(df, starts_with="a", contains="f", ends_with="b"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["afb"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 8: Using 'or' Operator"}
        >>> print(get_columns_by_likeness(df, starts_with="a", operator="or", contains="f"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aaa", "aab", "aac", "afa", "afb", "afc"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 9: Using 'and not' Operator"}
        >>> print(get_columns_by_likeness(df, starts_with="a", operator="and not", contains="f"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["aaa", "aab", "aac"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 10: Error Example 1"}
        >>> print(get_columns_by_likeness(df, starts_with=123))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        TypeError: `starts_with` must be a `string` or `None`.
        ```
        !!! failure "Conclusion: Error."
        </div>

        ```{.py .python linenums="1" title="Example 11: Error Example 2"}
        >>> print(get_columns_by_likeness(df, operator="xor"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ValueError: `operator` must be one of 'and', 'or', 'and not', 'or not'
        ```
        !!! failure "Conclusion: Error."
        </div>
    """

    # Columns
    cols: str_list = dataframe.columns
    if not match_case:
        cols = [col.upper() for col in cols]
        starts_with = starts_with.upper() if starts_with is not None else None
        contains = contains.upper() if contains is not None else None
        ends_with = ends_with.upper() if ends_with is not None else None

    # Parameters
    o_: Literal["and", "or", "and not", "or not"] = operator
    s_: bool = starts_with is not None
    c_: bool = contains is not None
    e_: bool = ends_with is not None

    # Functions
    _ops = {
        "and": lambda x, y: x and y,
        "or": lambda x, y: x or y,
        "and not": lambda x, y: x and not y,
        "or not": lambda x, y: x or not y,
    }
    _s = lambda col, s: col.startswith(s)
    _c = lambda col, c: c in col
    _e = lambda col, e: col.endswith(e)
    _sc = lambda col, s, c: _ops[o_](_s(col, s), _c(col, c))
    _se = lambda col, s, e: _ops[o_](_s(col, s), _e(col, e))
    _ce = lambda col, c, e: _ops[o_](_c(col, c), _e(col, e))
    _sce = lambda col, s, c, e: _ops[o_](_ops[o_](_s(col, s), _c(col, c)), _e(col, e))

    # Logic
    if s_ and not c_ and not e_:
        return [col for col in cols if _s(col, starts_with)]
    elif c_ and not s_ and not e_:
        return [col for col in cols if _c(col, contains)]
    elif e_ and not s_ and not c_:
        return [col for col in cols if _e(col, ends_with)]
    elif s_ and c_ and not e_:
        return [col for col in cols if _sc(col, starts_with, contains)]
    elif s_ and e_ and not c_:
        return [col for col in cols if _se(col, starts_with, ends_with)]
    elif c_ and e_ and not s_:
        return [col for col in cols if _ce(col, contains, ends_with)]
    elif s_ and c_ and e_:
        return [col for col in cols if _sce(col, starts_with, contains, ends_with)]
    else:
        return cols


# ---------------------------------------------------------------------------- #
#  Renaming                                                                 ####
# ---------------------------------------------------------------------------- #


@typechecked
def rename_columns(
    dataframe: psDataFrame,
    columns: Optional[Union[str, str_collection]] = None,
    string_function: str = "upper",
) -> psDataFrame:
    """
    !!! note "Summary"
        Use one of the common Python string functions to be applied to one or multiple columns.

    ???+ abstract "Details"
        The `string_function` must be a valid string method. For more info on available functions, see: https://docs.python.org/3/library/stdtypes.html#string-methods

    Params:
        dataframe (psDataFrame):
            The DataFrame to be updated.
        columns (Optional[Union[str, str_collection]], optional):
            The columns to be updated.<br>
            Must be a valid column on `dataframe`.<br>
            If not provided, will be applied to all columns.<br>
            It is also possible to parse the values `"all"`, which will also apply this function to all columns in `dataframe`.<br>
            Defaults to `None`.
        string_function (str, optional):
            The string function to be applied. Defaults to `"upper"`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Import
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.columns import rename_columns
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

        ```{.py .python linenums="1" title="Example 1: Single column, default params"}
        >>> print(rename_columns(df, "a").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["A", "b", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 2: Single column, simple function"}
        >>> print(rename_columns(df, "a", "upper").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["A", "b", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 3: Single column, complex function"}
        >>> print(rename_columns(df, "a", "replace('b', 'test')").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a", "test", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 4: Multiple columns"}
        >>> print(rename_columns(df, ["a", "b"]).columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["A", "B", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 5: Default function over all columns"}
        >>> print(rename_columns(df).columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["A", "B", "C", "D"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 6: Complex function over multiple columns"}
        >>> print(rename_columns(df, ["a", "b"], "replace('b', 'test')").columns)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["a", "test", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

    ??? tip "See Also"
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`assert_column_exists()`][toolbox_pyspark.checks.assert_column_exists]
    """
    columns = get_columns(dataframe, columns)
    assert_columns_exists(dataframe=dataframe, columns=columns, match_case=True)
    cols_exprs: dict[str, str] = {
        col: eval(
            f"'{col}'.{string_function}{'()' if not string_function.endswith(')') else ''}"
        )
        for col in columns
    }
    return dataframe.withColumnsRenamed(cols_exprs)


# ---------------------------------------------------------------------------- #
#  Reordering                                                               ####
# ---------------------------------------------------------------------------- #


@typechecked
def reorder_columns(
    dataframe: psDataFrame,
    new_order: Optional[str_collection] = None,
    missing_columns_last: bool = True,
    key_columns_position: Optional[Literal["first", "last"]] = "first",
) -> psDataFrame:
    """
    !!! note "Summary"
        Reorder the columns in a given DataFrame in to a custom order, or to put the `key_` columns at the end (that is, to the far right) of the dataframe.

    ???+ abstract "Details"
        The decision flow chart is as follows:

        ```mermaid
        graph TD
            a([begin])
            z([end])
            b{{new_order}}
            c{{missing_cols_last}}
            d{{key_cols_position}}
            g[cols = dataframe.columns]
            h[cols = new_order]
            i[cols += missing_cols]
            j[cols = non_key_cols + key_cols]
            k[cols = key_cols + non_key_cols]
            l["return dataframe.select(cols)"]
            a --> b
            b --is not None--> h --> c
            b --is None--> g --> d
            c --False--> l
            c --True--> i ----> l
            d --"first"--> k ---> l
            d --"last"---> j --> l
            d --None--> l
            l --> z
        ```

    Params:
        dataframe (psDataFrame):
            The DataFrame to update
        new_order (Optional[Union[str, str_list, str_tuple, str_set]], optional):
            The custom order for the columns on the order.<br>
            Defaults to `#!py None`.
        missing_columns_last (bool, optional):
            For any columns existing on `#!py dataframes.columns`, but missing from `#!py new_order`, if `#!py missing_columns_last=True`, then include those missing columns to the right of the dataframe, in the same order that they originally appear.<br>
            Defaults to `#!py True`.
        key_columns_position (Optional[Literal["first", "last"]], optional):
            Where should the `#!py "key_*"` columns be located?.<br>

            - If `#!py "first"`, then they will be relocated to the start of the dataframe, before all other columns.
            - If `#!py "last"`, then they will be relocated to the end of the dataframe, after all other columns.
            - If `#!py None`, they they will remain their original order.

            Regardless of their position, their original order will be maintained.
            Defaults to `#!py "first"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.columns import reorder_columns
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
        ...             "key_a": ["0", "1", "2", "3"],
        ...             "c": ["1", "1", "1", "1"],
        ...             "d": ["2", "2", "2", "2"],
        ...             "key_c": ["1", "1", "1", "1"],
        ...             "key_e": ["3", "3", "3", "3"],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-------+---+---+-------+-------+
        | a | b | key_a | c | d | key_c | key_e |
        +---+---+-------+---+---+-------+-------+
        | 0 | a |     0 | 1 | 2 |     1 |     3 |
        | 1 | b |     1 | 1 | 2 |     1 |     3 |
        | 2 | c |     2 | 1 | 2 |     1 |     3 |
        | 3 | d |     3 | 1 | 2 |     1 |     3 |
        +---+---+-------+---+---+-------+-------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Default config"}
        >>> new_df = reorder_columns(dataframe=df)
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-------+-------+-------+---+---+---+---+
        | key_a | key_c | key_e | a | b | c | d |
        +-------+-------+-------+---+---+---+---+
        |     0 |     1 |     3 | 0 | a | 1 | 2 |
        |     1 |     1 |     3 | 1 | b | 1 | 2 |
        |     2 |     1 |     3 | 2 | c | 1 | 2 |
        |     3 |     1 |     3 | 3 | d | 1 | 2 |
        +-------+-------+-------+---+---+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 2: Custom order"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "b", "key_e", "a", "c", "d"],
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-------+-------+---+-------+---+---+---+
        | key_a | key_c | b | key_e | a | c | d |
        +-------+-------+---+-------+---+---+---+
        |     0 |     1 | a |     3 | 0 | 1 | 2 |
        |     1 |     1 | b |     3 | 1 | 1 | 2 |
        |     2 |     1 | c |     3 | 2 | 1 | 2 |
        |     3 |     1 | d |     3 | 3 | 1 | 2 |
        +-------+-------+---+-------+---+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 3: Custom order, include missing columns"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "a", "b"],
        ...     missing_columns_last=True,
        ...     )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-------+-------+---+---+-------+---+---+
        | key_a | key_c | a | b | key_e | c | d |
        +-------+-------+---+---+-------+---+---+
        |     0 |     1 | 0 | a |     3 | 1 | 2 |
        |     1 |     1 | 1 | b |     3 | 1 | 2 |
        |     2 |     1 | 2 | c |     3 | 1 | 2 |
        |     3 |     1 | 3 | d |     3 | 1 | 2 |
        +-------+-------+---+---+-------+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 4: Custom order, exclude missing columns"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "a", "b"],
        ...     missing_columns_last=False,
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-------+-------+---+---+
        | key_a | key_c | a | b |
        +-------+-------+---+---+
        |     0 |     1 | 0 | a |
        |     1 |     1 | 1 | b |
        |     2 |     1 | 2 | c |
        |     3 |     1 | 3 | d |
        +-------+-------+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 5: Keys last"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     key_columns_position="last",
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+-------+-------+-------+
        | a | b | c | d | key_a | key_c | key_e |
        +---+---+---+---+-------+-------+-------+
        | 0 | a | 1 | 2 |     0 |     1 |     3 |
        | 1 | b | 1 | 2 |     1 |     1 |     3 |
        | 2 | c | 1 | 2 |     2 |     1 |     3 |
        | 3 | d | 1 | 2 |     3 |     1 |     3 |
        +---+---+---+---+-------+-------+-------+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 6: Keys first"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     key_columns_position="first",
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +-------+-------+-------+---+---+---+---+
        | key_a | key_c | key_e | a | b | c | d |
        +-------+-------+-------+---+---+---+---+
        |     0 |     1 |     3 | 0 | a | 1 | 2 |
        |     1 |     1 |     3 | 1 | b | 1 | 2 |
        |     2 |     1 |     3 | 2 | c | 1 | 2 |
        |     3 |     1 |     3 | 3 | d | 1 | 2 |
        +-------+-------+-------+---+---+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>
    """
    df_cols: str_list = dataframe.columns
    if new_order is not None:
        cols: str_list = get_columns(dataframe, new_order)
        if missing_columns_last:
            cols += [col for col in df_cols if col not in new_order]
    else:
        non_key_cols: str_list = [col for col in df_cols if not col.lower().startswith("key_")]
        key_cols: str_list = [col for col in df_cols if col.lower().startswith("key_")]
        if key_columns_position == "first":
            cols = key_cols + non_key_cols
        elif key_columns_position == "last":
            cols = non_key_cols + key_cols
        else:
            cols = df_cols
    return dataframe.select(cols)


# ---------------------------------------------------------------------------- #
#  Deleting                                                                 ####
# ---------------------------------------------------------------------------- #


@typechecked
def delete_columns(
    dataframe: psDataFrame,
    columns: Union[str, str_collection],
    missing_column_handler: Literal["raise", "warn", "pass"] = "pass",
) -> psDataFrame:
    """
    !!! note "Summary"
        For a given `#!py dataframe`, delete the columns listed in `columns`.

    ???+ abstract "Details"
        You can use `#!py missing_columns_handler` to specify how to handle missing columns.

    Params:
        dataframe (psDataFrame):
            The dataframe from which to delete the columns
        columns (Union[str, str_collection]):
            The list of columns to delete.
        missing_column_handler (Literal["raise", "warn", "pass"], optional):
            How to handle any columns which are missing from `#!py dataframe.columns`.

            If _any_ columns in `columns` are missing from `#!py dataframe.columns`, then the following will happen for each option:

            | Option | Result |
            |--------|--------|
            | `#!py "raise"` | An `#!py ColumnDoesNotExistError` exception will be raised
            | `#!py "warn"` | An `#!py ColumnDoesNotExistWarning` warning will be raised
            | `#!py "pass"` | Nothing will be raised

            Defaults to `#!py "pass"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the `#!py columns` do not exist within `#!py dataframe.columns`.

    Returns:
        (psDataFrame):
            The updated `#!py dataframe`, with the columns listed in `#!py columns` having been removed.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.columns import delete_columns
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

        ```{.py .python linenums="1" title="Example 1: Single column"}
        >>> df.transform(delete_columns, "a").show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+
        | b | c | d |
        +---+---+---+
        | a | c | d |
        | b | c | d |
        | c | c | d |
        | d | c | d |
        +---+---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 2: Multiple columns"}
        >>> df.transform(delete_columns, ["a", "b"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 3: Single column missing, raises error"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns="z",
        ...         missing_column_handler="raise",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ["z"] do not exist in "dataframe".
        Try one of: ["a", "b", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 4: Multiple columns, one missing, raises error"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["a", "b", "z"],
        ...         missing_column_handler="raise",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ["z"] do not exist in "dataframe".
        Try one of: ["a", "b", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 5: Multiple columns, all missing, raises error"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["x", "y", "z"],
        ...         missing_column_handler="raise",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ["x", "y", "z"] do not exist in "dataframe".
        Try one of: ["a", "b", "c", "d"]
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 6: Single column missing, raises warning"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns="z",
        ...         missing_column_handler="warn",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistWarning: Columns missing from "dataframe": ["z"].
        Will still proceed to delete columns that do exist.
        ```
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
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 7: Multiple columns, one missing, raises warning"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["a", "b", "z"],
        ...         missing_column_handler="warn",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistWarning: Columns missing from "dataframe": ["z"].
        Will still proceed to delete columns that do exist.
        ```
        ```{.txt .text title="Terminal"}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 8: Multiple columns, all missing, raises warning"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["x", "y", "z"],
        ...         missing_column_handler="warn",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistWarning: Columns missing from "dataframe": ["x", "y", "z"].
        Will still proceed to delete columns that do exist.
        ```
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
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 9: Single column missing, nothing raised"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns="z",
        ...         missing_column_handler="pass",
        ...     )
        ...     .show()
        ... )
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
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 10: Multiple columns, one missing, nothing raised"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["a", "b", "z"],
        ...         missing_column_handler="pass",
        ...     )
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        !!! success "Conclusion: Success."
        </div>

        ```{.py .python linenums="1" title="Example 11: Multiple columns, all missing, nothing raised"}
        >>> (
        ...     df.transform(
        ...         delete_columns,
        ...         columns=["x", "y", "z"],
        ...         missing_column_handler="pass",
        ...     )
        ...     .show()
        ... )
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
        !!! success "Conclusion: Success."
        </div>
    """
    columns = get_columns(dataframe, columns)
    if missing_column_handler == "raise":
        assert_columns_exists(dataframe=dataframe, columns=columns)
    elif missing_column_handler == "warn":
        warn_columns_missing(dataframe=dataframe, columns=columns)
    elif missing_column_handler == "pass":
        pass
    return dataframe.select([col for col in dataframe.columns if col not in columns])
