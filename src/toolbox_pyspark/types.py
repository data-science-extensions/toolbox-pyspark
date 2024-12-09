# ============================================================================ #
#                                                                              #
#     Title   : Column Types                                                   #
#     Purpose : Get, check, and change a datafames column data types.          #
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
    The `types` module is used to get, check, and change a datafames column data types.
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
# Python StdLib Imports
from typing import Union

# ## Python Third Party Imports ----
# Python Open Source Imports
import pandas as pd
from pandas import DataFrame as pdDataFrame
from pyspark.sql import (
    DataFrame as psDataFrame,
    functions as F,
    types as T,
)
from toolbox_python.collection_types import str_list
from toolbox_python.dictionaries import dict_reverse_keys_and_values
from typeguard import typechecked

# ## Local Module Imports ----
from .checks import (
    assert_column_exists,
    assert_columns_exists,
)
from .constants import (
    VALID_DATAFRAME_NAMES,
    VALID_PYSPARK_DATAFRAME_NAMES,
)


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: list[str] = [
    "get_column_types",
    "cast_column_to_type",
    "cast_columns_to_type",
    "map_cast_columns_to_type",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Private functions                                                        ####
# ---------------------------------------------------------------------------- #


def _validate_pyspark_datatype(datatype: Union[str, type, T.DataType]):
    datatype = T.FloatType() if datatype == "float" or datatype is float else datatype
    if isinstance(datatype, str):
        datatype = "string" if datatype == "str" else datatype
        datatype = "boolean" if datatype == "bool" else datatype
        datatype = "integer" if datatype == "int" else datatype
        datatype = "timestamp" if datatype == "datetime" else datatype
        try:
            datatype = eval(datatype)
        except NameError:
            datatype = T._parse_datatype_string(s=datatype)  # type:ignore
        else:
            pass
    if type(datatype).__name__ == "type":
        datatype = T._type_mappings.get(datatype)()  # type:ignore
    return datatype


# ---------------------------------------------------------------------------- #
#  Public functions                                                         ####
# ---------------------------------------------------------------------------- #


@typechecked
def get_column_types(
    dataframe: psDataFrame,
    output_type: str = "psDataFrame",
) -> Union[psDataFrame, pdDataFrame]:
    """
    !!! note "Summary"
        This is also a convenient function to return the data types from a given table as either a `pyspark.DataFrame` or `pandas.DataFrame`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be checked.

        output_type (str, optional):
            How should the data be returned? As `pd.DataFrame` or `ps.DataFrame`.

            For `pandas`, use one of:

            - `"pandas.DataFrame"`
            - `"pandas"`
            - `"pd.DataFrame"`
            - `"pd.df"`
            - `"pddf"`
            - `"pdDataFrame"`
            - `"pdDF"`
            - `"pd"`

            For `pyspark` use one of:

            - `"spark.DataFrame"`
            - `"pyspark.DataFrame"`
            - `"pyspark"`
            - `"spark"`
            - `"ps.DataFrame"`
            - `"ps.df"`
            - `"psdf"`
            - `"psDataFrame"`
            - `"psDF"`
            - `"ps"`

            Any other options are invalid.<br>
            Defaults to `#!py "psDataFrame"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the given value parsed to `output_type` is not one of the given valid types.

    Returns:
        (Union[psDataFrame, pdDataFrame]):
            The DataFrame where each row represents a column on the original `dataframe` object, and which has two columns:

            1. The column name from `dataframe`; and
            2. The data type for that column in `dataframe`.

    ???+ example "Examples"

        For PySpark:
        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import get_column_types
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...         'c': [1,1,1,1],
        ...         'd': ['2','2','2','2'],
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> print(df.dtypes)
        ```
        <div class="result" markdown>
        ```{.py .python}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "bigint"),
            ("d", "string"),
        ]
        ```
        </div>

        ```{.py .python linenums="1" title="Return PySpark"}
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="Return Pandas"}
        >>> print(get_column_types(df, "pd"))
        ```
        <div class="result" markdown>
        ```{.txt .text}
           col_name  col_type
        0         a    bigint
        1         b    string
        2         c    bigint
        3         d    string
        ```
        </div>
    """
    if output_type not in VALID_DATAFRAME_NAMES:
        raise AttributeError(
            f"Invalid value for `output_type`: '{output_type}\n"
            f"Must be one of: {VALID_DATAFRAME_NAMES}"
        )
    output = pd.DataFrame(dataframe.dtypes, columns=["col_name", "col_type"])
    if output_type in VALID_PYSPARK_DATAFRAME_NAMES:
        return dataframe.sparkSession.createDataFrame(output)
    else:
        return output


@typechecked
def cast_column_to_type(
    dataframe: psDataFrame,
    column: str,
    datatype: Union[str, type, T.DataType],
) -> psDataFrame:
    """
    !!! note "Summary"
        This is a convenience function for casting a single column on a given table to another data type.

    ???+ abstract "Details"
        At it's core, it will call the function like this:
        ```{.py .python linenums="1"}
        dataframe = dataframe.withColumn(column, F.col(column).cast(datatype))
        ```
        The reason for wrapping it up in this function is for validation of a columns existence and convenient re-declaration of the same.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be updated.
        column (str):
            The column to be updated.
        datatype (Union[str, Type, T.DataType]):
            The datatype to be cast to.
            Must be a valid `pyspark` DataType. One of the following:
            ```{.py .python}
            [
                "string",
                "char",
                "varchar",
                "binary",
                "boolean",
                "decimal",
                "float",
                "double",
                "byte",
                "short",
                "integer",
                "long",
                "date",
                "timestamp",
                "timestamp_ntz",
                "void",
            ]
            ```

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
        >>> from toolbox_pyspark.types import cast_column_to_type, get_column_types
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...         'c': [1,1,1,1],
        ...         'd': ['2','2','2','2'],
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        </div>
        ```{.py .python linenums="1" title="Basic usage"}
        >>> df = cast_column_to_type(df, 'a', 'string')
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

    ??? tip "See Also"
        - [`assert_column_exists()`][toolbox_pyspark.checks.column_exists]
        - [`is_vaid_spark_type()`][toolbox_pyspark.checks.is_vaid_spark_type]
        - [`get_column_types()`][toolbox_pyspark.types.get_column_types]
    """
    assert_column_exists(dataframe, column)
    datatype = _validate_pyspark_datatype(datatype=datatype)
    return dataframe.withColumn(column, F.col(column).cast(datatype))  # type:ignore


@typechecked
def cast_columns_to_type(
    dataframe: psDataFrame,
    columns: Union[str, str_list],
    datatype: Union[str, type, T.DataType],
) -> psDataFrame:
    """
    !!! note "Summary"
        Cast multiple columns to a given type.

    ???+ abstract "Details"
        An extension of [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type].

    Params:
        dataframe (psDataFrame):
            The DataFrame to be updated.
        columns (Union[str, List[str]]):
            The list of columns to be updated. They all must be valid columns existing on `DataFrame`.
        datatype (Union[str, Type, T.DataType]):
            The datatype to be cast to.
            Must be a valid PySpark DataType. One of the following:
                ```{.py .python}
                [
                    "string",
                    "char",
                    "varchar",
                    "binary",
                    "boolean",
                    "decimal",
                    "float",
                    "double",
                    "byte",
                    "short",
                    "integer",
                    "long",
                    "date",
                    "timestamp",
                    "timestamp_ntz",
                    "void",
                ]
                ```

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
        >>> from toolbox_pyspark.types import cast_columns_to_type, get_column_types
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...         'c': [1,1,1,1],
        ...         'd': [2,2,2,2],
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | bigint   |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="Basic usage"}
        >>> df = cast_column_to_type(df, ['a'], 'string')
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | bigint   |
        | d        | bigint   |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple columns"}
        >>> df = cast_column_to_type(df, ['c', 'd'], 'string')
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

    ??? info "Notes"
        The reason why this function will call the [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] function under the hood, instead of directly parse'ing to the `pyspark` DataFrame method `.withColumns()` is because the [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] contains some additional validation steps on the `datatype` parameter (like converting `#!py str` to `string`), which is not logical to re-write here in this wrapper function.

    ??? tip "See Also"
        - [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type]
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`is_vaid_spark_type()`][toolbox_pyspark.checks.is_vaid_spark_type]
        - [`get_column_types()`][toolbox_pyspark.types.get_column_types]
    """
    columns = [columns] if isinstance(columns, str) else columns
    assert_columns_exists(dataframe, columns)
    datatype = _validate_pyspark_datatype(datatype=datatype)
    return dataframe.withColumns({col: F.col(col).cast(datatype) for col in columns})


@typechecked
def map_cast_columns_to_type(
    dataframe: psDataFrame,
    columns_type_mapping: dict[
        Union[str, type, T.DataType],
        Union[str, list[str], tuple[str, ...]],
    ],
) -> psDataFrame:
    """
    !!! note "Summary"
        Take a dictionary mapping of where the keys is the type and the values are the column(s), and apply that to the given dataframe.

    ???+ abstract "Details"
        Applies [`cast_columns_to_type()`][toolbox_pyspark.types.cast_columns_to_type] and [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] under the hood.

    Params:
        dataframe (psDataFrame):
            The DataFrame to transform.
        columns_type_mapping (Dict[ Union[str, Type, T.DataType], Union[str, List[str], Tuple[str, ...]], ]):
            The mapping of the columns to manipulate.<br>
            The format must be: `#!py {type: columns}`.<br>
            Where the keys are the relevant type to cast to, and the values are the column(s) for casting.

    Returns:
        (psDataFrame):
            The transformed data frame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import map_cast_columns_to_type, get_column_types
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame(
        ...     pd.DataFrame({
        ...         'a': [1,2,3,4],
        ...         'b': ['a','b','c','d'],
        ...         'c': [1,1,1,1],
        ...         'd': ['2','2','2','2'],
        ...     })
        ... )
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="Basic usage"}
        >>> df = map_cast_columns_to_type(df, {"str": ["a", "c"]})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="Multiple types"}
        >>> df = map_cast_columns_to_type(df, {"int": ["a", "c"], "str": ["b"], "float": "d"})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | float    |
        +----------+----------+
        ```
        </div>

        ```{.py .python linenums="1" title="All to single type"}
        >>> df = map_cast_columns_to_type(df, {str: [col for col in df.columns]})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        </div>

    ??? tip "See Also"
        - [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type]
        - [`cast_columns_to_type()`][toolbox_pyspark.types.cast_columns_to_type]
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`is_vaid_spark_type()`][toolbox_pyspark.checks.is_vaid_spark_type]
        - [`get_column_types()`][toolbox_pyspark.types.get_column_types]
    """

    # Ensure all keys are `str`
    keys = (*columns_type_mapping.keys(),)
    for key in keys:
        if isinstance(key, type):
            if key.__name__ in keys:
                columns_type_mapping[key.__name__] = list(
                    columns_type_mapping[key.__name__]
                ) + list(columns_type_mapping.pop(key))
            else:
                columns_type_mapping[key.__name__] = columns_type_mapping.pop(key)

    # Reverse keys and values
    reversed_mapping = dict_reverse_keys_and_values(dictionary=columns_type_mapping)

    # Apply mapping to dataframe
    try:
        dataframe = dataframe.withColumns(
            {
                col: F.col(col).cast(_validate_pyspark_datatype(typ))
                for col, typ in reversed_mapping.items()
            }
        )
    except Exception as e:
        raise RuntimeError(f"Raised {e.__class__.__name__}: {e}") from e

    # Return
    return dataframe
