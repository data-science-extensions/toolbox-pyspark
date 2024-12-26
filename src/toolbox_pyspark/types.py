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
from typing import Union

# ## Python Third Party Imports ----
import pandas as pd
from pandas import DataFrame as pdDataFrame
from pyspark.sql import DataFrame as psDataFrame, functions as F, types as T
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_list, str_tuple
from toolbox_python.dictionaries import dict_reverse_keys_and_values
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import (
    _validate_pyspark_datatype,
    assert_column_exists,
    assert_columns_exists,
)
from toolbox_pyspark.constants import (
    VALID_DATAFRAME_NAMES,
    VALID_PYSPARK_DATAFRAME_NAMES,
)
from toolbox_pyspark.utils.exceptions import InvalidDataFrameNameError


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
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
#  Public functions                                                         ####
# ---------------------------------------------------------------------------- #


@typechecked
def get_column_types(
    dataframe: psDataFrame,
    output_type: str = "psDataFrame",
) -> Union[psDataFrame, pdDataFrame]:
    """
    !!! note "Summary"
        This is a convenient function to return the data types from a given table as either a `#!py pyspark.sql.DataFrame` or `#!py pandas.DataFrame`.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be checked.

        output_type (str, optional):
            How should the data be returned? As `#!py pdDataFrame` or `#!py psDataFrame`.

            For `#!py pandas`, use one of:

            ```{.sh .shell  title="Terminal"}
            [
                "pandas", "pandas.DataFrame",
                "pd.df",  "pd.DataFrame",
                "pddf",   "pdDataFrame",
                "pd",     "pdDF",
            ]
            ```

            </div>

            For `#!py pyspark` use one of:

            ```{.sh .shell  title="Terminal"}
            [
                "pyspark", "spark.DataFrame",
                "spark",   "pyspark.DataFrame",
                "ps.df",   "ps.DataFrame",
                "psdf",    "psDataFrame",
                "ps",      "psDF",
            ]
            ```

            Any other options are invalid.<br>
            Defaults to `#!py "psDataFrame"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        InvalidPySparkDataTypeError:
            If the given value parsed to `#!py output_type` is not one of the given valid types.

    Returns:
        (Union[psDataFrame, pdDataFrame]):
            The DataFrame where each row represents a column on the original `#!py dataframe` object, and which has two columns:

            1. The column name from `#!py dataframe`; and
            2. The data type for that column in `#!py dataframe`.

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
        >>> print(df.dtypes)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "bigint"),
            ("d", "string"),
        ]
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Return PySpark"}
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully print PySpark output."
        </div>

        ```{.py .python linenums="1" title="Example 2: Return Pandas"}
        >>> print(get_column_types(df, "pd"))
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
           col_name  col_type
        0         a    bigint
        1         b    string
        2         c    bigint
        3         d    string
        ```
        !!! success "Conclusion: Successfully print Pandas output."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid output"}
        >>> print(get_column_types(df, "foo"))
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        InvalidDataFrameNameError: Invalid value for `output_type`: "foo".
        Must be one of: ["pandas.DataFrame", "pandas", "pd.DataFrame", "pd.df", "pddf", "pdDataFrame", "pdDF", "pd", "spark.DataFrame", "pyspark.DataFrame", "pyspark", "spark", "ps.DataFrame", "ps.df", "psdf", "psDataFrame", "psDF", "ps"]
        ```
        !!! failure "Conclusion: Invalid input."
        </div>
    """
    if output_type not in VALID_DATAFRAME_NAMES:
        raise InvalidDataFrameNameError(
            f"Invalid value for `output_type`: '{output_type}'.\n"
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
        datatype (Union[str, type, T.DataType]):
            The datatype to be cast to.
            Must be a valid `#!py pyspark` DataType.

            Use one of the following:
            ```{.sh .shell  title="Terminal"}
            [
                "string",  "char",
                "varchar", "binary",
                "boolean", "decimal",
                "float",   "double",
                "byte",    "short",
                "integer", "long",
                "date",    "timestamp",
                "void",    "timestamp_ntz",
            ]
            ```

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ColumnDoesNotExistError:
            If the `#!py column` does not exist within `#!py dataframe.columns`.
        ParseException:
            If the given `#!py datatype` is not a valid PySpark DataType.

    Returns:
        (psDataFrame):
            The updated DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import cast_column_to_type, get_column_types
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
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
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

        ```{.py .python linenums="1" title="Example 1: Valid casting"}
        >>> df = cast_column_to_type(df, "a", "string")
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | bigint   |
        | d        | string   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast column to type."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid column"}
        >>> df = cast_column_to_type(df, "x", "string")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Column "x" does not exist in DataFrame.
        Try one of: ["a", "b", "c", "d"].
        ```
        !!! failure "Conclusion: Column `x` does not exist as a valid column."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid datatype"}
        >>> df = cast_column_to_type(df, "b", "foo")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ParseException: DataType "foo" is not supported.
        ```
        !!! failure "Conclusion: Datatype `foo` is not valid."
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
        An extension of [`#!py cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] to allow casting of multiple columns simultaneously.

    Params:
        dataframe (psDataFrame):
            The DataFrame to be updated.
        columns (Union[str, str_list]):
            The list of columns to be updated. They all must be valid columns existing on `#!py DataFrame`.
        datatype (Union[str, type, T.DataType]):
            The datatype to be cast to.
            Must be a valid PySpark DataType.

            Use one of the following:
                ```{.sh .shell  title="Terminal"}
                [
                    "string",  "char",
                    "varchar", "binary",
                    "boolean", "decimal",
                    "float",   "double",
                    "byte",    "short",
                    "integer", "long",
                    "date",    "timestamp",
                    "void",    "timestamp_ntz",
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
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import cast_column_to_type, get_column_types
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
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
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

        ```{.py .python linenums="1" title="Example 1: Basic usage"}
        >>> df = cast_column_to_type(df, ["a"], "string")
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | bigint   |
        | d        | bigint   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast column to type."
        </div>

        ```{.py .python linenums="1" title="Example 2: Multiple columns"}
        >>> df = cast_column_to_type(df, ["c", "d"], "string")
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast columns to type."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid column"}
        >>> df = cast_columns_to_type(df, ["x", "y"], "string")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ColumnDoesNotExistError: Columns ["x", "y"] do not exist in DataFrame.
        Try one of: ["a", "b", "c", "d"].
        ```
        !!! failure "Conclusion: Columns `[x]` does not exist as a valid column."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid datatype"}
        >>> df = cast_columns_to_type(df, ["a", "b"], "foo")
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ParseException: DataType "foo" is not supported.
        ```
        !!! failure "Conclusion: Datatype `foo` is not valid."
        </div>

    ??? tip "See Also"
        - [`assert_columns_exists()`][toolbox_pyspark.checks.assert_columns_exists]
        - [`is_vaid_spark_type()`][toolbox_pyspark.checks.is_vaid_spark_type]
        - [`get_column_types()`][toolbox_pyspark.types.get_column_types]
    """
    columns = [columns] if is_type(columns, str) else columns
    assert_columns_exists(dataframe, columns)
    datatype = _validate_pyspark_datatype(datatype=datatype)
    return dataframe.withColumns({col: F.col(col).cast(datatype) for col in columns})


@typechecked
def map_cast_columns_to_type(
    dataframe: psDataFrame,
    columns_type_mapping: dict[
        Union[str, type, T.DataType],
        Union[str, str_list, str_tuple],
    ],
) -> psDataFrame:
    """
    !!! note "Summary"
        Take a dictionary mapping of where the keys is the type and the values are the column(s), and apply that to the given dataframe.

    ???+ abstract "Details"
        Applies [`#!py cast_columns_to_type()`][toolbox_pyspark.types.cast_columns_to_type] and [`#!py cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] under the hood.

    Params:
        dataframe (psDataFrame):
            The DataFrame to transform.
        columns_type_mapping (Dict[ Union[str, type, T.DataType], Union[str, str_list, str_tuple], ]):
            The mapping of the columns to manipulate.<br>
            The format must be: `#!py {type: columns}`.<br>
            Where the keys are the relevant type to cast to, and the values are the column(s) for casting.

    Returns:
        (psDataFrame):
            The transformed data frame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.types import cast_column_to_type, get_column_types
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
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
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

        ```{.py .python linenums="1" title="Example 1: Basic usage"}
        >>> df = map_cast_columns_to_type(df, {"str": ["a", "c"]})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast columns to type."
        </div>

        ```{.py .python linenums="1" title="Example 2: Multiple types"}
        >>> df = map_cast_columns_to_type(df, {"int": ["a", "c"], "str": ["b"], "float": "d"})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | bigint   |
        | b        | string   |
        | c        | bigint   |
        | d        | float    |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast columns to types."
        </div>

        ```{.py .python linenums="1" title="Example 3: All to single type"}
        >>> df = map_cast_columns_to_type(df, {str: [col for col in df.columns]})
        >>> get_column_types(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +----------+----------+
        | col_name | col_type |
        +----------+----------+
        | a        | string   |
        | b        | string   |
        | c        | string   |
        | d        | string   |
        +----------+----------+
        ```
        !!! success "Conclusion: Successfully cast all columns to type."
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
        if is_type(key, type):
            if key.__name__ in keys:
                columns_type_mapping[key.__name__] = list(
                    columns_type_mapping[key.__name__]
                ) + list(columns_type_mapping.pop(key))
            else:
                columns_type_mapping[key.__name__] = columns_type_mapping.pop(key)

    # Reverse keys and values
    reversed_mapping = dict_reverse_keys_and_values(dictionary=columns_type_mapping)

    # Validate
    assert_columns_exists(dataframe, reversed_mapping.keys())

    # Apply mapping to dataframe
    try:
        dataframe = dataframe.withColumns(
            {
                col: F.col(col).cast(_validate_pyspark_datatype(typ))
                for col, typ in reversed_mapping.items()
            }
        )
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"Raised {e.__class__.__name__}: {e}") from e

    # Return
    return dataframe
