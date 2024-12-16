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
import warnings
from typing import Literal, Optional, Union

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame
from toolbox_python.collection_types import str_collection, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import _columns_exists, assert_columns_exists
from toolbox_pyspark.utils.warnings import AttributeWarning


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
        columns (Optional[Union[str, List[str], Tuple[str, ...]]], optional):
            Optional filter criteria for selecting columns.<br>
            If a string is provided, it can be one of the following options:

            - `#!py "all"`: Return all columns in the DataFrame.
            - `#!py "all_str"`: Return columns of string type.
            - `#!py "all_int"`: Return columns of integer type.
            - `#!py "all_numeric"`: Return columns of numeric types (integers and floats).
            - `#!py "all_datetime"` or 'all timestamp': Return columns of datetime or timestamp type.
            - `#!py "all_date"`: Return columns of date type.
            - Any other string: Return columns matching the provided exact column name.

            If a list or tuple of column names is provided, return only those columns.<br>
            Defaults to `#!py None` (which returns all columns).

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (List[str]):
            The selected column names from the DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from pyspark_helpers.columns import get_columns
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pdDataFrame(
        ...         {
        ...             "a": range(0,1,2,3),
        ...             "b": ['a','b','c','d'],
        ...         }
        ...     )
        ... ).withColumns(
        ...     {
        ...         "c": F.lit("1").cast("int"),
        ...         "d": F.lit("2").cast("string"),
        ...         "e": F.lit("1.1").cast("float"),
        ...         "f": F.lit("1.2").cast("double"),
        ...         "g": F.lit("2022-01-01").cast("date"),
        ...         "h": F.lit("2022-02-01 01:00:00").cast("timestamp"),
        ...     }
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +---+---+---+---+-----+-----+------------+---------------------+
        | a | b | c | d |   e |   f |          g |                   h |
        +---+---+---+---+-----+-----+------------+---------------------+
        | 0 | a | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 1 | b | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 2 | c | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 3 | d | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        +---+---+---+---+-----+-----+------------+---------------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Default params"}
        >>> print(get_columns(df).columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        ```
        </div>

        ```{.py .python linenums="1" title="Specific columns"}
        >>> print(get_columns(df, ["a", "b", "c"]).columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["a", "b", "c"]
        ```
        </div>

        ```{.py .python linenums="1" title="Single column as list"}
        >>> print(get_columns(df, ["a"]).columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["a"]
        ```
        </div>

        ```{.py .python linenums="1" title="Single column as string"}
        >>> print(get_columns(df, "a").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["a"]
        ```
        </div>

        ```{.py .python linenums="1" title="All columns"}
        >>> print(get_columns(df, "all").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        ```
        </div>

        ```{.py .python linenums="1" title="All str"}
        >>> print(get_columns(df, "all_str").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['b', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="All int"}
        >>> print(get_columns(df, "all int").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['c']
        ```
        </div>

        ```{.py .python linenums="1" title="All float"}
        >>> print(get_columns(df, "all_decimal").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['e','f']
        ```
        </div>

        ```{.py .python linenums="1" title="All numeric"}
        >>> print(get_columns(df, "all_numeric").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['c','e','f']
        ```
        </div>

        ```{.py .python linenums="1" title="All date"}
        >>> print(get_columns(df, "all_date").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['g']
        ```
        </div>

        ```{.py .python linenums="1" title="All datetime"}
        >>> print(get_columns(df, "all_datetime").columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['h']
        ```
        </div>
    """
    if columns is None:
        return dataframe.columns
    elif isinstance(columns, str):
        if "all" in columns:
            if "str" in columns:
                return [
                    col for col, typ in dataframe.dtypes if typ in ["str", "string"]
                ]
            elif "int" in columns:
                return [
                    col for col, typ in dataframe.dtypes if typ in ["int", "integer"]
                ]
            elif "numeric" in columns:
                return [
                    col
                    for col, typ in dataframe.dtypes
                    if typ in ["int", "integer", "float", "double", "long"]
                    or "decimal" in typ
                ]
            elif "float" in columns or "double" in columns or "decimal" in columns:
                return [
                    col
                    for col, typ in dataframe.dtypes
                    if typ in ["float", "double", "long"] or "decimal" in typ
                ]
            elif "datetime" in columns or "timestamp" in columns:
                return [
                    col
                    for col, typ in dataframe.dtypes
                    if typ in ["datetime", "timestamp"]
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

    !!! deprecation "ToDo"
        Update examples in function docstring to explain how the `operator` parameter works.

    ???+ abstract "Details"
        You can use any combination of `startswith`, `contains`, and `endswith`. Under the hood, these will be implemented with a number of internal `#!py lambda` functions to determine matches.

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
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from pyspark_helpers.columns import get_columns_by_likeness
        >>> spark = SparkSession.builder.getOrCreate()
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
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Starts With"}
        >>> get_columns_by_likeness(df, starts_with="a")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["aaa", "aab", "aac", "afa", "afb", "afc"]
        ```
        </div>

        ```{.py .python linenums="1" title="Contains"}
        >>> get_columns_by_likeness(df, contains="f")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["afa", "afb", "afc"]
        ```
        </div>

        ```{.py .python linenums="1" title="Third"}
        >>> get_columns_by_likeness(df, ends_with="c")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["aac", "afc", "bac"]
        ```
        </div>

        ```{.py .python linenums="1" title="Starts With and Contains"}
        >>> get_columns_by_likeness(df, starts_with="a", contains="c")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["aac", "afc"]
        ```
        </div>

        ```{.py .python linenums="1" title="Starts With and Ends With"}
        >>> get_columns_by_likeness(df, starts_with="a", ends_with="b")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["aab", "afb"]
        ```
        </div>

        ```{.py .python linenums="1" title="Contains and Ends With"}
        >>> get_columns_by_likeness(df, contains="f", ends_with="b")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["afb"]
        ```
        </div>

        ```{.py .python linenums="1" title="Starts With and Contains and Ends With"}
        >>> get_columns_by_likeness(df, starts_with="a", contains="f", ends_with="b")
        ```
        <div class="result" markdown>
        ```{.py .python}
        ["afb"]
        ```
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
        columns (Optional[Union[str, str_list, str_tuple, str_set]], optional):
            The columns to be updated.<br>
            Must be a valid column on `dataframe`.<br>
            If not provided, will be applied to all columns.<br>
            It is also possible to parse the values `'all'`, which will also apply this function to all columns in `dataframe`.<br>
            Defaults to `None`.
        string_function (str, optional):
            The string function to be applied. Defaults to `"upper"`.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from pyspark_helpers.columns import rename_columns
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0,1,2,3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ['c','c','c','c'],
        ...             "d": ['d','d','d','d'],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Single column, default params"}
        >>> new_df = rename_columns(df, "a")
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['A', 'b', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Single column, simple function"}
        >>> new_df = rename_columns(df, "a", "upper")
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['A', 'b', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Single column, complex function"}
        >>> new_df = rename_columns(df, "a", "replace('b', 'test')")
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['a', 'test', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns"}
        >>> new_df = rename_columns(df, ['a', 'b']")
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['A', 'B', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Default function over all columns"}
        >>> new_df = rename_columns(df)
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['A', 'B', 'C', 'D']
        ```
        </div>

        ```{.py .python linenums="1" title="Complex function over multiple columns"}
        >>> new_df = rename_columns(df, ['a', 'b'], "replace('b', 'test')")
        >>> print(new_df.columns)
        ```
        <div class="result" markdown>
        ```{.py .python}
        ['a', 'test', 'c', 'd']
        ```
        </div>

    ??? tip "See Also"
        - [`assert_columns_exists()`][pyspark_helpers.checks.assert_columns_exists]
        - [`assert_column_exists()`][pyspark_helpers.checks.assert_column_exists]
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
    key_columns_last: bool = True,
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
            c --True--> i ---> l
            c --False--> l
            d --'first'--> k ---> l
            d --'last'---> j --> l
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
        key_columns_last (bool, optional):
            !!! deprecation "Deprecated"
                Parameter `key_columns_last` is deprecated in `v1.18.2` in favour of `key_columns_position`.<br>
                It will be removed in `v1.19.0`.
        key_columns_position (Optional[Literal["first", "last"]], optional):
            Where should the `#!py 'key_*'` columns be located?.<br>

            - If `#!py 'first'`, then they will be relocated to the start of the dataframe, before all other columns.
            - If `#!py 'last'`, then they will be relocated to the end of the dataframe, after all other columns.
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
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from pyspark_helpers.columns import reorder_columns
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0,1,2,3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "key_a": ['0','1','2','3'],
        ...             "c": ['1','1','1','1'],
        ...             "d": ['2','2','2','2'],
        ...             "key_c": ['1','1','1','1'],
        ...             "key_e": ['3','3','3','3'],
        ...         }
        ...     )
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Default config"}
        >>> new_df = reorder_columns(dataframe=df)
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +-------+-------+-------+---+---+---+---+
        | key_a | key_c | key_e | a | b | c | d |
        +-------+-------+-------+---+---+---+---+
        |     0 |     1 |     3 | 0 | a | 1 | 2 |
        |     1 |     1 |     3 | 1 | b | 1 | 2 |
        |     2 |     1 |     3 | 2 | c | 1 | 2 |
        |     3 |     1 |     3 | 3 | d | 1 | 2 |
        +-------+-------+-------+---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Custom order"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "b", "key_e", "a", "c", "d"],
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +-------+-------+---+-------+---+---+---+
        | key_a | key_c | b | key_e | a | c | d |
        +-------+-------+---+-------+---+---+---+
        |     0 |     1 | a |     3 | 0 | 1 | 2 |
        |     1 |     1 | b |     3 | 1 | 1 | 2 |
        |     2 |     1 | c |     3 | 2 | 1 | 2 |
        |     3 |     1 | d |     3 | 3 | 1 | 2 |
        +-------+-------+---+-------+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Custom order, include missing columns"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "a", "b"],
        ...     missing_columns_last=True,
        ...     )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +-------+-------+---+---+-------+---+---+
        | key_a | key_c | a | b | key_e | c | d |
        +-------+-------+---+---+-------+---+---+
        |     0 |     1 | 0 | a |     3 | 1 | 2 |
        |     1 |     1 | 1 | b |     3 | 1 | 2 |
        |     2 |     1 | 2 | c |     3 | 1 | 2 |
        |     3 |     1 | 3 | d |     3 | 1 | 2 |
        +-------+-------+---+---+-------+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Custom order, exclude missing columns"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     new_order=["key_a", "key_c", "a", "b"],
        ...     missing_columns_last=False,
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +-------+-------+---+---+
        | key_a | key_c | a | b |
        +-------+-------+---+---+
        |     0 |     1 | 0 | a |
        |     1 |     1 | 1 | b |
        |     2 |     1 | 2 | c |
        |     3 |     1 | 3 | d |
        +-------+-------+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Keys last"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     key_columns_position="last",
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +---+---+---+---+-------+-------+-------+
        | a | b | c | d | key_a | key_c | key_e |
        +---+---+---+---+-------+-------+-------+
        | 0 | a | 1 | 2 |     0 |     1 |     3 |
        | 1 | b | 1 | 2 |     1 |     1 |     3 |
        | 2 | c | 1 | 2 |     2 |     1 |     3 |
        | 3 | d | 1 | 2 |     3 |     1 |     3 |
        +---+---+---+---+-------+-------+-------+
        ```
        </div>

        ```{.py .python linenums="1" title="Keys first"}
        >>> new_df = reorder_columns(
        ...     dataframe=df,
        ...     key_columns_position="first",
        ... )
        >>> new_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +-------+-------+-------+---+---+---+---+
        | key_a | key_c | key_e | a | b | c | d |
        +-------+-------+-------+---+---+---+---+
        |     0 |     1 |     3 | 0 | a | 1 | 2 |
        |     1 |     1 |     3 | 1 | b | 1 | 2 |
        |     2 |     1 |     3 | 2 | c | 1 | 2 |
        |     3 |     1 |     3 | 3 | d | 1 | 2 |
        +-------+-------+-------+---+---+---+---+
        ```
        </div>
    """
    df_cols: str_list = dataframe.columns
    if new_order is not None:
        cols: str_list = get_columns(dataframe, new_order)
        if missing_columns_last:
            cols += [col for col in df_cols if col not in new_order]
    else:
        non_key_cols: str_list = [
            col for col in df_cols if not col.lower().startswith("key_")
        ]
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
        columns (Union[str, str_list, str_tuple, str_set]):
            The list of columns to delete.
        missing_column_handler (Literal["raise", "warn", "pass"], optional):
            How to handle any columns which are missing from `#!py dataframe.columns`.

            If _any_ columns in `columns` are missing from `#!py dataframe.columns`, then the following will happen for each option:

            - If `#!py "raise"` then an `#!py AttributeError` exception will be raised
            - If `#!py "warn"` then an `#!py AttributeWarning` warning will be raised
            - If `#!py "pass"`, then nothing will be raised

            Defaults to `#!py "pass"`.

    Returns:
        (psDataFrame):
            The updated `#!py dataframe`, with the columns listed in `#!py columns` having been removed.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from pyspark_helpers.columns import delete_columns
        >>> spark = SparkSession.builder.getOrCreate()
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
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Single column"}
        >>> df.transform(delete_columns, "a").show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +---+---+---+
        | b | c | d |
        +---+---+---+
        | a | c | d |
        | b | c | d |
        | c | c | d |
        | d | c | d |
        +---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns"}
        >>> df.transform(delete_columns, ["a", "b"]).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Single column missing, raises error"}
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
        ```{.txt .text}
        AttributeError: Columns ['z'] do not exist in 'dataframe'.
        Try one of: ['a', 'b', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns, one missing, raises error"}
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
        ```{.txt .text}
        AttributeError: Columns ['z'] do not exist in 'dataframe'.
        Try one of: ['a', 'b', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns, all missing, raises error"}
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
        ```{.txt .text}
        AttributeError: Columns ['x', 'y', 'z'] do not exist in 'dataframe'.
        Try one of: ['a', 'b', 'c', 'd']
        ```
        </div>

        ```{.py .python linenums="1" title="Single column missing, raises warning"}
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
        ```{.txt .text}
        AttributeWarning: Columns missing from 'dataframe': ['z'].
        Will still proceed to delete columns that do exist.
        ```
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Multiple columns, one missing, raises warning"}
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
        ```{.txt .text}
        AttributeWarning: Columns missing from 'dataframe': ['z'].
        Will still proceed to delete columns that do exist.
        ```
        ```{.txt .text}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns, all missing, raises warning"}
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
        ```{.txt .text}
        AttributeWarning: Columns missing from 'dataframe': ['x', 'y', 'z'].
        Will still proceed to delete columns that do exist.
        ```
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Single column missing, nothing raised"}
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
        ```{.txt .text}
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

        ```{.py .python linenums="1" title="Multiple columns, one missing, nothing raised"}
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
        ```{.txt .text}
        +---+---+
        | c | d |
        +---+---+
        | c | d |
        | c | d |
        | c | d |
        | c | d |
        +---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns, all missing, nothing raised"}
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
        ```{.txt .text}
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
    """
    columns = get_columns(dataframe, columns)
    if missing_column_handler == "raise":
        assert_columns_exists(dataframe=dataframe, columns=columns)
    elif missing_column_handler == "warn":
        exists, missing_cols = _columns_exists(dataframe=dataframe, columns=columns)
        if not exists:
            warnings.warn(
                f"Columns missing from 'dataframe': {missing_cols}.\n"
                f"Will still proceed to delete columns that do exist",
                AttributeWarning,
            )
    elif missing_column_handler == "pass":
        pass
    return dataframe.select([col for col in dataframe.columns if col not in columns])
