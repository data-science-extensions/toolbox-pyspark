# ============================================================================ #
#                                                                              #
#     Title   : Dimensions                                                     #
#     Purpose : Check the dimensions of a`pyspark` `dataframes`.               #
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
    The `dimensions` module is used for checking the dimensions of `pyspark` `dataframe`'s.
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
from copy import deepcopy
from typing import Dict, Optional, Union

# ## Python Third Party Imports ----
import numpy as np
from pandas import DataFrame as pdDataFrame
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_list
from typeguard import typechecked


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "get_dims",
    "get_dims_of_tables",
    "make_dimension_table",
    "replace_columns_with_dimension_id",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Functions                                                                ####
# ---------------------------------------------------------------------------- #


@typechecked
def get_dims(
    dataframe: psDataFrame,
    use_names: bool = True,
    use_comma: bool = True,
) -> Union[dict[str, str], dict[str, int], tuple[str, str], tuple[int, int]]:
    """
    !!! note "Summary"
        Extract the dimensions of a given `dataframe`.

    Params:
        dataframe (psDataFrame):
            The table to check.
        use_names (bool, optional):
            Whether or not to add `names` to the returned object.<br>
            If `#!py True`, then will return a `#!py dict` with two keys only, for the number of `rows` and `cols`.<br>
            Defaults to `#!py True`.
        use_comma (bool, optional):
            Whether or not to add a comma `,` to the returned object.<br>
            Defaults to `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (Union[Dict[str, Union[str, int]], tuple[str, ...], tTuple[int, ...]]):
            The dimensions of the given `dataframe`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.dimensions import get_dims
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': range(5000),
        ...         'b': range(5000),
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> print(df.count())
        >>> print(len(df.columns))
        ```
        <div class="result" markdown>
        ```{.txt .text}
        5000
        ```

        ```{.txt .text}
        2
        ```
        </div>

        ```{.py .python linenums="1" title="Names and commas"}
        >>> print(get_dims(dataframe=df, use_names=True, use_commas=True))
        ```
        <div class="result" markdown>
        ```{.sh .shell  title="Terminal"}
        {"rows": "5,000", "cols": "2"}
        ```
        </div>

        ```{.py .python linenums="1" title="Names but no commas"}
        >>> print(get_dims(dataframe=df, use_names=True, use_commas=False))
        ```
        <div class="result" markdown>
        ```{.sh .shell  title="Terminal"}
        {"rows": 5000, "cols": 2}
        ```
        </div>

        ```{.py .python linenums="1" title="Commas but no names"}
        >>> print(get_dims(dataframe=df, use_names=False, use_commas=True))
        ```
        <div class="result" markdown>
        ```{.sh .shell  title="Terminal"}
        ("5,000", "2")
        ```
        </div>

        ```{.py .python linenums="1" title="Neither names nor commas"}
        >>> print(get_dims(dataframe=df, use_names=False, use_commas=False))
        ```
        <div class="result" markdown>
        ```{.sh .shell  title="Terminal"}
        (5000, 2)
        ```
        </div>
    """
    dims: tuple[int, int] = (dataframe.count(), len(dataframe.columns))
    if use_names and use_comma:
        return {"rows": f"{dims[0]:,}", "cols": f"{dims[1]:,}"}
    elif use_names and not use_comma:
        return {"rows": dims[0], "cols": dims[1]}
    elif not use_names and use_comma:
        return (f"{dims[0]:,}", f"{dims[1]:,}")
    else:
        return dims


@typechecked
def get_dims_of_tables(
    tables: str_list,
    scope: Optional[dict] = None,
    use_comma: bool = True,
) -> pdDataFrame:
    """
    !!! note "Summary"
        Take in a list of the names of some tables, and for each of them, check their dimensions.

    ???+ abstract "Details"
        This function will check against the `#!py global()` scope. So you need to be careful if you're dealing with massive amounts of data in memory.

    Params:
        tables (List[str]):
            The list of the tables that will be checked.
        scope (dict, optional):
            This is the scope against which the tables will be checked.<br>
            If `#!py None`, then it will use the `#!py global()` scope by default..<br>
            Defaults to `#!py None`.
        use_comma (bool, optional):
            Whether or not the dimensions from the tables should be formatted as a string with a comma as the thousandths delimiter.<br>
            Defaults to `#!py True`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (pdDataFrame):
            A `pandas` `dataframe` with four columns: `#!py ["table", "type", "rows", "cols"]`.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.dimensions import get_dims_of_tables, get_dims
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df1 = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': range(5000),
        ...         'b': range(5000),
        ...     })
        ... )
        >>> df2 = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': range(10000),
        ...         'b': range(10000),
        ...         'c': range(10000),
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> print(get_dims(df1))
        >>> print(get_dims(df1))
        ```
        <div class="result" markdown>
        ```{.txt .text}
        {"rows": "5000", "cols": "2"}
        ```

        ```{.txt .text}
        {"rows": "10000", "cols": "3"}
        ```
        </div>

        ```{.py .python linenums="1" title="Basic usage"}
        >>> print(get_dims_of_tables(['df1', 'df2']))
        ```
        <div class="result" markdown>
        ```{.txt .text}
          table type  rows cols
        0   df1      5,000    2
        1   df2      1,000    3
        ```
        </div>

        ```{.py .python linenums="1" title="No commas"}
        >>> print(get_dims_of_tables(['df1', 'df2'], use_commas=False))
        ```
        <div class="result" markdown>
        ```{.txt .text}
          table type rows cols
        0   df1      5000    2
        1   df2      1000    3
        ```
        </div>

        ```{.py .python linenums="1" title="Missing DF"}
        >>> display(get_dims_of_tables(['df1', 'df2', 'df3'], use_comma=False))
        ```
        <div class="result" markdown>
        ```{.txt .text}
          table type rows cols
        0   df1      5000    2
        1   df2      1000    3
        1   df3       NaN  NaN
        ```
        </div>

    ??? info "Notes"
        - The first column of the returned table is the name of the table from the `scope` provided.
        - The second column of the returned table is the `type` of the table. That is, whether the table is one of `#!py ["prd", "arc", "acm"]`, which are for 'production', 'archive', accumulation' categories. This is designated by the table containing an underscore (`_`), and having a suffic of either one of: `#!py "prd"`, `#!py "arc"`, or `#!py "acm"`. If the table does not contain this info, then the value in this second column will just be blank.
        - If one of the tables given in the `tables` list does not exist in the `scope`, then the values given in the `rows` and `cols` columns will either be the values: `#!py np.nan` or `#!py "Did not load"`.
    """
    sizes: Dict[str, list] = {
        "table": list(),
        "type": list(),
        "rows": list(),
        "cols": list(),
    }
    rows: Union[str, int, float]
    cols: Union[str, int, float]
    for tbl, typ in [
        (
            table.rsplit("_", 1)
            if "_" in table and table.endswith(("acm", "arc", "prd"))
            else (table, "")
        )
        for table in tables
    ]:
        try:
            tmp: psDataFrame = eval(
                f"{tbl}{f'_{typ}' if typ!='' else ''}",
                globals() if scope is None else scope,
            )
            rows, cols = get_dims(tmp, use_names=False, use_comma=use_comma)
        except Exception:
            if use_comma:
                rows = cols = "Did not load"
            else:
                rows = cols = np.nan
        sizes["table"].append(tbl)
        sizes["type"].append(typ)
        sizes["rows"].append(rows)
        sizes["cols"].append(cols)
    return pdDataFrame(sizes)


def make_dimension_table(
    dataframe: psDataFrame,
    columns: Union[str, list[str]],
    index_prefix: str = "id",
) -> psDataFrame:
    """
    !!! note "Summary"
        Create a dimension table from the specified columns of a given `pyspark` dataframe.

    ???+ abstract "Details"
        This function will create a dimension table from the specified columns of a given `pyspark` dataframe. The dimension table will contain the unique values of the specified columns, along with an index column that will be used to replace the original columns in the original dataframe.

        index column will be named according to the `index_prefix` parameter. If only one column is specified, then the index column will be named according to the `index_prefix` parameter followed by the name of the column. If multiple columns are specified, then the index column will be named according to the `index_prefix` parameter only. The index column will be created by using the `#!py row_number()` window function over the specified columns.

        The dimension table will be created by selecting the specified columns from the original dataframe, then applying the `#!py distinct()` function to get the unique values, and finally applying the `#!py row_number()` window function to create the index column.

    Params:
        dataframe (psDataFrame):
            The DataFrame to create the dimension table from.
        columns (Union[str, str_collection]):
            The column(s) to include in the dimension table.
        index_prefix (str, optional):
            The prefix to use for the index column.<br>
            Defaults to `#!py "id"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the columns specified do not exist in the dataframe.

    Returns:
        (psDataFrame):
            The dimension table.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.dimensions import make_dimension_table
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
        ...             "d": ["a", "b", "b", "b"],
        ...             "e": ["x", "x", "y", "z"],
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
        | 1 | a | 1 | a | x |
        | 2 | b | 1 | b | x |
        | 3 | c | 2 | b | y |
        | 4 | d | 2 | b | z |
        +---+---+---+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Create dimension table with single column"}
        >>> dim_table = make_dimension_table(df, "d")
        >>> dim_table.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +------+---+
        | id_d | d |
        +------+---+
        |    1 | a |
        |    2 | b |
        +------+---+
        ```
        !!! success "Conclusion: Successfully created dimension table with single column."
        </div>

        ```{.py .python linenums="1" title="Example 2: Create dimension table with multiple columns"}
        >>> dim_table = make_dimension_table(df, ["c", "d"])
        >>> dim_table.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----+---+---+
        | id | c | d |
        +----+---+---+
        |  1 | 1 | a |
        |  2 | 1 | b |
        |  3 | 2 | b |
        +----+---+---+
        ```
        !!! success "Conclusion: Successfully created dimension table with multiple columns."
        </div>

        ```{.py .python linenums="1" title="Example 3: Use different prefix"}
        >>> dim_table = make_dimension_table(df, "e", "index")
        >>> dim_table.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---------+---+
        | index_e | e |
        +---------+---+
        |       1 | x |
        |       2 | y |
        |       3 | z |
        +---------+---+
        ```
        !!! success "Conclusion: Successfully created dimension table with different prefix."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid column"}
        >>> dim_table = make_dimension_table(df, "123")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column '123' does not exist in the DataFrame.
        ```
        !!! failure "Conclusion: Failed to create dimension table due to invalid column name."
        </div>

    ??? tip "See Also"
        - [`replace_columns_with_dimension_id`][toolbox_pyspark.dimensions.replace_columns_with_dimension_id]
    """
    columns = [columns] if is_type(columns, str) else columns
    index_name: str = f"{index_prefix}_{columns[0]}" if len(columns) == 0 else index_prefix
    return (
        dataframe.select(*columns)
        .distinct()
        .withColumn(index_name, F.expr(f"row_number() over (order by {', '.join(columns)})"))
        .select(index_name, *columns)
    )


def replace_columns_with_dimension_id(
    fct_dataframe: psDataFrame,
    dim_dataframe: psDataFrame,
    cols_to_replace: Union[str, list[str]],
    dim_id_col: Optional[str] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Replace the specified columns in a given `pyspark` dataframe with the corresponding dimension table IDs.

    ???+ abstract "Details"
        This function will replace the specified columns in a given `pyspark` dataframe with the corresponding dimension table IDs. The dimension table IDs will be obtained by joining the dimension table with the original dataframe on the specified columns. The original columns will then be dropped from the original dataframe.

        The dimension table IDs will be added to the original dataframe to replace the columns specified in `cols_to_replace`. The dimension table IDs will be obtained by joining the dimension table with the original dataframe on the specified columns.

        The join will be performed using a left join, so that any rows in the original dataframe that do not have a corresponding row in the dimension table will have a `#!sql null` value for the dimension table ID. The original columns will be dropped from the original dataframe after the join.         The resulting dataframe will have the same number of rows as the original dataframe, but with the specified columns replaced by the dimension table IDs.

    Params:
        fct_dataframe (psDataFrame):
            The DataFrame to replace the columns in.
        dim_dataframe (psDataFrame):
            The dimension table containing the IDs.
        cols_to_replace (Union[str, str_collection]):
            The column(s) to replace with the dimension table IDs.
        dim_id_col (str, optional):
            The name of the column in the dimension table containing the IDs.<br>
            If `#!py None`, then will use the first column of the dimension table.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If any of the columns specified do not exist in the dataframes.

    Returns:
        (psDataFrame):
            The DataFrame with the columns replaced by the dimension table IDs.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>>
        >>> # Instantiate Spark
        >>> from toolbox_pyspark.dimensions import make_dimension_table, replace_columns_with_dimension_id
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [1, 2, 3, 4],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": [1, 1, 2, 2],
        ...             "d": ["a", "b", "b", "b"],
        ...             "e": ["x", "x", "y", "z"],
        ...         }
        ...     )
        ... )
        >>> dim_table1 = make_dimension_table(df, "d")
        >>> dim_table2 = make_dimension_table(df, "e")
        >>> dim_table3 = make_dimension_table(df, ("c", "d"))
        >>>
        >>> # Check
        >>> df.show()
        >>> dim_table1.show()
        >>> dim_table2.show()
        >>> dim_table3.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | a | x |
        | 2 | b | 1 | b | x |
        | 3 | c | 2 | b | y |
        | 4 | d | 2 | b | z |
        +---+---+---+---+---+
        ```
        ```{.txt .text title="Terminal"}
        +------+---+
        | id_d | d |
        +------+---+
        |    1 | a |
        |    2 | b |
        +------+---+
        ```
        ```{.txt .text title="Terminal"}
        +------+---+
        | id_e | e |
        +------+---+
        |    1 | x |
        |    2 | y |
        |    3 | z |
        +------+---+
        ```
        ```{.txt .text title="Terminal"}
        +----+---+---+
        | id | c | d |
        +----+---+---+
        |  1 | 1 | a |
        |  2 | 1 | b |
        |  3 | 2 | b |
        +----+---+---+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Replace single column with dimension ID"}
        >>> df_replaced = replace_columns_with_dimension_id(df, dim_table1, "d")
        >>> df_replaced.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+------+---+
        | a | b | c | id_d | e |
        +---+---+---+------+---+
        | 1 | a | 1 |    1 | x |
        | 2 | b | 1 |    2 | x |
        | 3 | c | 2 |    2 | y |
        | 4 | d | 2 |    2 | z |
        +---+---+---+------+---+
        ```
        !!! success "Conclusion: Successfully replaced single column with dimension ID."
        </div>

        ```{.py .python linenums="1" title="Example 2: Replace single column with dimension ID"}
        >>> df_replaced = replace_columns_with_dimension_id(df, dim_table2, "e")
        >>> df_replaced.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+---+------+
        | a | b | c | d | id_e |
        +---+---+---+---+------+
        | 1 | a | 1 | a |    1 |
        | 2 | b | 1 | b |    1 |
        | 3 | c | 2 | b |    2 |
        | 4 | d | 2 | b |    3 |
        +---+---+---+---+------+
        ```
        !!! success "Conclusion: Successfully replaced single column with dimension ID."
        </div>

        ```{.py .python linenums="1" title="Example 3: Replace multiple columns with dimension IDs"}
        >>> df_replaced_multi = replace_columns_with_dimension_id(df, dim_table3, ["c", "d"])
        >>> df_replaced_multi.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+----+---+
        | a | b | id | e |
        +---+---+----+---+
        | 1 | a |  1 | x |
        | 2 | b |  2 | x |
        | 3 | c |  3 | y |
        | 4 | d |  3 | z |
        +---+---+----+---+
        ```
        !!! success "Conclusion: Successfully replaced multiple columns with dimension IDs."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid column type"}
        >>> df_replaced = replace_columns_with_dimension_id(df, dim_table, "123")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column '123' does not exist in the DataFrame.
        ```
        !!! failure "Conclusion: Failed to replace columns due to invalid column type."
        </div>

    ??? tip "See Also"
        - [`make_dimension_table`][toolbox_pyspark.dimensions.make_dimension_table]
    """
    index_of_first_col: int = fct_cols.index(cols_to_replace[0])
    fct_new_cols: list[str] = deepcopy(fct_cols)
    fct_new_cols = list(
        (
            *fct_new_cols[:index_of_first_col],
            dim_id_col,
            *fct_new_cols[index_of_first_col + 1 :],
        )
    )
    fct_removed_cols: list[str] = [col for col in fct_new_cols if col not in cols_to_replace]
    return (
        fct_dataframe.alias("a")
        .join(
            other=dim_dataframe.alias("b"),
            on=[F.col(f"a.{col}") == F.col(f"b.{col}") for col in cols_to_replace],
            how="left",
        )
        .select("a.*", f"b.{dim_id_col}")
        .select(fct_removed_cols)
    )
