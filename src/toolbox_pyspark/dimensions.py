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
from typing import Dict, Optional, Union

# ## Python Third Party Imports ----
import numpy as np
from pandas import DataFrame as pdDataFrame
from pyspark.sql import DataFrame as psDataFrame
from toolbox_python.collection_types import str_list
from typeguard import typechecked


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = ["get_dims", "get_dims_of_tables"]


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
