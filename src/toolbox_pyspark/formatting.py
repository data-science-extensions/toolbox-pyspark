# ============================================================================ #
#                                                                              #
#     Title: Title                                                             #
#     Purpose: This module provides functions for formatting and displaying    #
#              intermediary Spark DataFrames.                                  #
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
    The `formatting` module provides functions for formatting and displaying.
"""


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Imports                                                                 ####
## --------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.collection_types import str_list
from typeguard import typechecked


## --------------------------------------------------------------------------- #
##  Exports                                                                 ####
## --------------------------------------------------------------------------- #


__all__: str_list = [
    "format_numbers",
    "display_intermediary_table",
    "display_intermediary_schema",
    "display_intermediary_columns",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Main Section                                                          ####
#                                                                              #
# ---------------------------------------------------------------------------- #


@typechecked
def format_numbers(dataframe: psDataFrame) -> psDataFrame:
    """
    !!! note "Summary"
        Format numbers in a Spark DataFrame.

    ??? abstract "Details"
        This function formats numbers in a Spark DataFrame. It formats integers to have no decimal places and floats to have two decimal places. The function is useful for displaying intermediary tables in a more readable format. It will replace all numeric columns to string.

    Params:
        dataframe (psDataFrame):
            The Spark DataFrame to format.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (psDataFrame):
            The formatted Spark DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set Up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.formatting import format_numbers
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
        ...             "c": [1.0, 2.0, 3.0, 4.0],
        ...             "d": [1.1, 2.2, 3.3, 4.4],
        ...             "e": [1000, 10000, 100000, 1000000],
        ...             "f": [1111.11, 22222.22, 333333.33, 4444444.44],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+---------+------------+
        | a | b |   c |   d |       e |          f |
        +---+---+-----+-----+---------+------------+
        | 1 | a | 1.0 | 1.1 |    1000 |    1111.11 |
        | 2 | b | 2.0 | 2.2 |   10000 |   22222.22 |
        | 3 | c | 3.0 | 3.3 |  100000 |  333333.33 |
        | 4 | d | 4.0 | 4.4 | 1000000 | 4444444.44 |
        +---+---+-----+-----+---------+------------+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Format Numbers by function"}
        >>> format_numbers(df).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+-----------+--------------+
        | a | b |   c |   d |         e |            f |
        +---+---+-----+-----+-----------+--------------+
        | 1 | a | 1.0 | 1.1 |     1,000 |     1,111.11 |
        | 2 | b | 2.0 | 2.2 |    10,000 |    22,222.22 |
        | 3 | c | 3.0 | 3.3 |   100,000 |   333,333.33 |
        | 4 | d | 4.0 | 4.4 | 1,000,000 | 4,444,444.44 |
        +---+---+-----+-----+-----------+--------------+
        ```
        !!! success "Conclusion: Successfully formatted dataframe.
        </div>

        ```{.py .python linenums="1" title="Example 2: Format Numbers by method"}
        >>> df.transform(format_numbers).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+-----------+--------------+
        | a | b |   c |   d |         e |            f |
        +---+---+-----+-----+-----------+--------------+
        | 1 | a | 1.0 | 1.1 |     1,000 |     1,111.11 |
        | 2 | b | 2.0 | 2.2 |    10,000 |    22,222.22 |
        | 3 | c | 3.0 | 3.3 |   100,000 |   333,333.33 |
        | 4 | d | 4.0 | 4.4 | 1,000,000 | 4,444,444.44 |
        +---+---+-----+-----+-----------+--------------+
        ```
        !!! success "Conclusion: Successfully formatted dataframe.
        </div>
    """
    for col, typ in dataframe.dtypes:
        if typ in ("int", "tinyint", "smallint", "bigint"):
            dataframe = dataframe.withColumn(col, F.format_number(col, 0))
        elif typ in ("float", "double"):
            dataframe = dataframe.withColumn(col, F.format_number(col, 2))
    return dataframe


@typechecked
def display_intermediary_table(
    dataframe: psDataFrame, reformat_numbers: bool = True, num_rows: int = 20
) -> psDataFrame:
    """
    !!! note "Summary"
        Display an intermediary Spark DataFrame.

    ???+ abstract "Details"
        This function displays an intermediary Spark DataFrame. The function is useful for displaying intermediary tables in a more readable format. Optionally, it can format numbers in the DataFrame to make it more readable.

    Params:
        dataframe (psDataFrame):
            The Spark DataFrame to display.
        reformat_numbers (bool):
            Whether to format numbers in the DataFrame. Default is `True`.
        num_rows (int):
            The number of rows to display. Default is `20`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (psDataFrame):
            The original Spark DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set Up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.formatting import display_intermediary_table
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
        ...             "c": [1.0, 2.0, 3.0, 4.0],
        ...             "d": [1.1, 2.2, 3.3, 4.4],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```

        ```{.py .python linenums="1" title="Example 1: Display intermediary table with no subsequent formatting"}
        >>> (
        ...     df
        ...     .transform(display_intermediary_table, reformat_numbers=False, num_rows=2)
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        +---+---+-----+-----+
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        !!! success "Conclusion: Successfully displayed intermediary table with no subsequent formatting.
        </div>

        ```{.py .python linenums="1" title="Example 2: Display intermediary table with subsequent formatting"}
        >>> (
        ...     df
        ...     .transform(display_intermediary_table, reformat_numbers=True)
        ...     .withColumn("c", F.expr("c * 2"))
        ...     .show()
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 2.0 | 1.1 |
        | 2 | b | 4.0 | 2.2 |
        | 3 | c | 6.0 | 3.3 |
        | 4 | d | 8.0 | 4.4 |
        +---+---+-----+-----+
        ```
        !!! success "Conclusion: Successfully displayed intermediary table with subsequent formatting.
        </div>
    """
    if reformat_numbers:
        dataframe.transform(format_numbers).show(n=num_rows, truncate=False)
    else:
        dataframe.show(n=num_rows, truncate=False)
    return dataframe


def display_intermediary_schema(dataframe: psDataFrame) -> psDataFrame:
    """
    !!! note "Summary"
        Display the schema of an intermediary Spark DataFrame.

    ??? abstract "Details"
        This function displays the schema of an intermediary Spark DataFrame. The function is useful for displaying intermediary tables in a more readable format.

    Params:
        dataframe (psDataFrame):
            The Spark DataFrame to display.

    Returns:
        (psDataFrame):
            The original Spark DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set Up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.formatting import display_intermediary_schema
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
        ...             "c": [1.0, 2.0, 3.0, 4.0],
        ...             "d": [1.1, 2.2, 3.3, 4.4],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        >>> df.printSchema()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        ```{.txt .text title="Terminal"}
        root
        |-- a: long (nullable = true)
        |-- b: string (nullable = true)
        |-- c: double (nullable = true)
        |-- d: double (nullable = true)
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Display intermediary schema"}
        >>> df.transform(display_intermediary_schema).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        root
        |-- a: long (nullable = true)
        |-- b: string (nullable = true)
        |-- c: double (nullable = true)
        |-- d: double (nullable = true)
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        !!! success "Conclusion: Successfully displayed intermediary schema.
        </div>

        ```{.py .python linenums="1" title="Example 2: Display intermediary schema with subsequent formatting"}
        >>> df.transform(display_intermediary_schema).withColumn("e", F.expr("c * 2")).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        root
        |-- a: long (nullable = true)
        |-- b: string (nullable = true)
        |-- c: double (nullable = true)
        |-- d: double (nullable = true)
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+---+
        | a | b |   c |   d | e |
        +---+---+-----+-----+---+
        | 1 | a | 1.0 | 1.1 | 2 |
        | 2 | b | 2.0 | 2.2 | 4 |
        | 3 | c | 3.0 | 3.3 | 6 |
        | 4 | d | 4.0 | 4.4 | 8 |
        +---+---+-----+-----+---+
        ```
    """
    dataframe.printSchema()
    return dataframe


def display_intermediary_columns(dataframe: psDataFrame) -> psDataFrame:
    """
    !!! note "Summary"
        Display the columns of an intermediary Spark DataFrame.

    ??? abstract "Details"
        This function displays the columns of an intermediary Spark DataFrame. The function is useful for displaying intermediary tables in a more readable format.

    Params:
        dataframe (psDataFrame):
            The Spark DataFrame to display.

    Returns:
        (psDataFrame):
            The original Spark DataFrame.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set Up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.formatting import display_intermediary_columns
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
        ...             "c": [1.0, 2.0, 3.0, 4.0],
        ...             "d": [1.1, 2.2, 3.3, 4.4],
        ...         }
        ...     )
        ... )
        >>>
        >>> # Check
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Display intermediary columns"}
        >>> df.transform(display_intermediary_columns).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ['a', 'b', 'c', 'd']
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+
        | a | b |   c |   d |
        +---+---+-----+-----+
        | 1 | a | 1.0 | 1.1 |
        | 2 | b | 2.0 | 2.2 |
        | 3 | c | 3.0 | 3.3 |
        | 4 | d | 4.0 | 4.4 |
        +---+---+-----+-----+
        ```
        !!! success "Conclusion: Successfully displayed intermediary columns.
        </div>

        ```{.py .python linenums="1" title="Example 2: Display intermediary columns with subsequent formatting"}
        >>> df.transform(display_intermediary_columns).withColumn("e", F.expr("c * 2")).show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ['a', 'b', 'c', 'd']
        ```
        ```{.txt .text title="Terminal"}
        +---+---+-----+-----+---+
        | a | b |   c |   d | e |
        +---+---+-----+-----+---+
        | 1 | a | 1.0 | 1.1 | 2 |
        | 2 | b | 2.0 | 2.2 | 4 |
        | 3 | c | 3.0 | 3.3 | 6 |
        | 4 | d | 4.0 | 4.4 | 8 |
        +---+---+-----+-----+---+
        ```
    """
    print(dataframe.columns)
    return dataframe
