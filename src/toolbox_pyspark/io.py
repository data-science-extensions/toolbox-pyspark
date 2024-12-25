# ============================================================================ #
#                                                                              #
#     Title   : IO                                                             #
#     Purpose : Read and write tables to/from directories.                     #
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
    The `io` module is used for reading and writing tables to/from directories.
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
from typing import Optional

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_dict, str_list
from typeguard import typechecked


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "read_from_path",
    "write_to_path",
    "transfer_table",
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
def read_from_path(
    name: str,
    path: str,
    spark_session: SparkSession,
    data_format: Optional[str] = "delta",
    read_options: Optional[str_dict] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Read an object from a given `path` in to memory as a `pyspark` dataframe.

    Params:
        name (str):
            The name of the table to read in.
        path (str):
            The path from which it will be read.
        spark_session (SparkSession):
            The Spark session to use for the reading.
        data_format (Optional[str], optional):
            The format of the object at location `path`.<br>
            Defaults to `#!py "delta"`.
        read_options (Dict[str, str], optional):
            Any additional obtions to parse to the Spark reader.<br>
            Like, for example:<br>

            - If the object is a CSV, you may want to define that it has a header row: `#!py {"header": "true"}`.
            - If the object is a Delta table, you may want to query a specific version: `#!py {versionOf": "0"}`.

            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#!py dict()`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (psDataFrame):
            The loaded dataframe.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import read_from_path
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = pd.DataFrame(
        ...     {
        ...         "a": [1, 2, 3, 4],
        ...         "b": ["a", "b", "c", "d"],
        ...         "c": [1, 1, 1, 1],
        ...         "d": ["2", "2", "2", "2"],
        ...     }
        ... )
        >>>
        >>> # Write data
        >>> df.to_csv("./test/table.csv")
        >>> df.to_parquet("./test/table.parquet")
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> import os
        >>> print(os.listdir("./test"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["table.csv", "table.parquet"]
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Read CSV"}
        >>> df_csv = read_from_path(
        ...     name="table.csv",
        ...     path="./test",
        ...     spark_session=spark,
        ...     data_format="csv",
        ...     options={"header": "true"},
        ... )
        >>>
        >>> df_csv.show()
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
        !!! success "Conclusion: Successfully read CSV."
        </div>

        ```{.py .python linenums="1" title="Example 2: Read Parquet"}
        >>> df_parquet = read_from_path(
        ...     name="table.parquet",
        ...     path="./test",
        ...     spark_session=spark,
        ...     data_format="parquet",
        ... )
        >>>
        >>> df_parquet.show()
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
        !!! success "Conclusion: Successfully read Parquet."
        </div>
    """
    data_format: str = data_format or "parquet"
    reader: DataFrameReader = spark_session.read.format(data_format)
    if read_options:
        reader.options(**read_options)
    return reader.load(f"{path}{'/' if not path.endswith('/') else ''}{name}")


@typechecked
def write_to_path(
    table: psDataFrame,
    name: str,
    path: str,
    data_format: Optional[str] = "delta",
    mode: Optional[str] = None,
    write_options: Optional[str_dict] = None,
    partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        For a given `table`, write it out to a specified `path` with name `name` and format `format`.

    Params:
        table (psDataFrame):
            The table to be written. Must be a valid `pyspark` DataFrame ([`pyspark.sql.DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)).
        name (str):
            The name of the table where it will be written.
        path (str):
            The path location for where to save the table.
        data_format (Optional[str], optional):
            The format that the `table` will be written to.<br>
            Defaults to `#!py "delta"`.
        mode (Optional[str], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        write_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            Like, for example:

            - If you are writing to a Delta object, and wanted to overwrite the schema: `#!py {"overwriteSchema": "true"}`.
            - If you"re writing to a CSV file, and wanted to specify the header row: `#!py {"header": "true"}`.

            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#!py dict()`.
        partition_cols (Optional[Union[str_collection, str]], optional):
            The column(s) that the table should partition by.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (type(None)):
            Nothing is returned.

    ???+ tip "Note"
        You know that this function is successful if the table exists at the specified location, and there are no errors thrown.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import write_to_path
        >>> from toolbox_pyspark.checks import table_exists
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
        ```

        ```{.py .python linenums="1" title="Check"}
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

        ```{.py .python linenums="1" title="Example 1: Write to CSV"}
        >>> write_to_path(
        ...     table=df,
        ...     name="df.csv",
        ...     path="./test",
        ...     data_format="csv",
        ...     mode="overwrite",
        ...     options={"header": "true"},
        ... )
        >>>
        >>> table_exists(
        ...     name="df.csv",
        ...     path="./test",
        ...     data_format="csv",
        ...     spark_session=df.sparkSession,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully written to CSV."
        </div>

        ```{.py .python linenums="1" title="Example 2: Write to Parquet"}
        >>> write_to_path(
        ...     table=df,
        ...     name="df.parquet",
        ...     path="./test",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ... )
        >>>
        >>> table_exists(
        ...     name="df.parquet",
        ...     path="./test",
        ...     data_format="parquet",
        ...     spark_session=df.sparkSession,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully written to Parquet."
        </div>
    """
    write_options: str_dict = write_options or dict()
    data_format: str = data_format or "parquet"
    writer: DataFrameWriter = table.write.mode(mode).format(data_format)
    if write_options:
        writer.options(**write_options)
    if partition_cols is not None:
        partition_cols = [partition_cols] if is_type(partition_cols, str) else partition_cols
        writer = writer.partitionBy(list(partition_cols))
    writer.save(f"{path}{'/' if not path.endswith('/') else ''}{name}")


@typechecked
def transfer_table(
    spark_session: SparkSession,
    from_table_path: str,
    from_table_name: str,
    from_table_format: str,
    to_table_path: str,
    to_table_name: str,
    to_table_format: str,
    from_table_options: Optional[str_dict] = None,
    to_table_mode: Optional[str] = None,
    to_table_options: Optional[str_dict] = None,
    to_table_partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        Copy a table from one location to another.

    ???+ abstract "Details"
        This is a blind transfer. There is no validation, no alteration, no adjustments made at all. Simply read directly from one location and move immediately to another location straight away.

    Params:
        spark_session (SparkSession):
            The spark session to use for the transfer. Necessary in order to instantiate the reading process.
        from_table_path (str):
            The path from which the table will be read.
        from_table_name (str):
            The name of the table to be read.
        from_table_format (str):
            The format of the data at the reading location.
        to_table_path (str):
            The location where to save the table to.
        to_table_name (str):
            The name of the table where it will be saved.
        to_table_format (str):
            The format of the saved table.
        from_table_options (Dict[str, str], optional):
            Any additional obtions to parse to the Spark reader.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#! dict()`.
        to_table_mode (Optional[str], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        to_table_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#! dict()`.
        to_table_partition_cols (Optional[Union[str_collection, str]], optional):
            The column(s) that the table should partition by.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (type(None)):
            Nothing is returned.

    ???+ tip "Note"
        You know that this function is successful if the table exists at the specified location, and there are no errors thrown.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import transfer_table
        >>> from toolbox_pyspark.checks import table_exists
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df = pd.DataFrame(
        ...     {
        ...         "a": [1, 2, 3, 4],
        ...         "b": ["a", "b", "c", "d"],
        ...         "c": [1, 1, 1 1],
        ...         "d": ["2", "2", "2", "2"],
        ...     }
        ... )
        >>> df.to_csv("./test/table.csv")
        >>> df.to_parquet("./test/table.parquet")
        ```

        ```{.py .python linenums="1" title="Check"}
        >>> import os
        >>> print(os.listdir("./test"))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        ["table.csv", "table.parquet"]
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Transfer CSV"}
        >>> transfer_table(
        ...     spark_session=spark,
        ...     from_table_path="./test",
        ...     from_table_name="table.csv",
        ...     from_table_format="csv",
        ...     to_table_path="./other",
        ...     to_table_name="table.csv",
        ...     to_table_format="csv",
        ...     from_table_options={"header": "true"},
        ...     to_table_mode="overwrite",
        ...     to_table_options={"header": "true"},
        ... )
        >>>
        >>> table_exists(
        ...     name="df.csv",
        ...     path="./other",
        ...     data_format="csv",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred CSV to CSV."
        </div>

        ```{.py .python linenums="1" title="Example 2: Transfer Parquet"}
        >>> transfer_table(
        ...     spark_session=spark,
        ...     from_table_path="./test",
        ...     from_table_name="table.parquet",
        ...     from_table_format="parquet",
        ...     to_table_path="./other",
        ...     to_table_name="table.parquet",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ...     to_table_options={"overwriteSchema": "true"},
        ... )
        >>>
        >>> table_exists(
        ...     name="df.parquet",
        ...     path="./other",
        ...     data_format="parquet",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred Parquet to Parquet."
        </div>

        ```{.py .python linenums="1" title="Example 3: Transfer CSV to Parquet"}
        >>> transfer_table(
        ...     spark_session=spark,
        ...     from_table_path="./test",
        ...     from_table_name="table.csv",
        ...     from_table_format="csv",
        ...     to_table_path="./other",
        ...     to_table_name="table.parquet",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ...     to_table_options={"overwriteSchema": "true"},
        ... )
        >>>
        >>> table_exists(
        ...     name="df.parquet",
        ...     path="./other",
        ...     data_format="parquet",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred CSV to Parquet."
        </div>
    """
    from_table_options: str_dict = from_table_options or dict()
    to_table_options: str_dict = to_table_options or dict()
    from_table: psDataFrame = read_from_path(
        name=from_table_name,
        path=from_table_path,
        spark_session=spark_session,
        data_format=from_table_format,
        read_options=from_table_options,
    )
    write_to_path(
        table=from_table,
        name=to_table_name,
        path=to_table_path,
        data_format=to_table_format,
        mode=to_table_mode,
        write_options=to_table_options,
        partition_cols=to_table_partition_cols,
    )
