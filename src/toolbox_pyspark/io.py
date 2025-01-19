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
from typing import Literal, Optional, get_args

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_dict, str_list, str_tuple
from typeguard import typechecked


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "read_from_path",
    "write_to_path",
    "transfer_table",
]


## --------------------------------------------------------------------------- #
##  Constants                                                               ####
## --------------------------------------------------------------------------- #


SPARK_FORMATS = Literal[
    # Built-in formats
    "parquet",
    "orc",
    "json",
    "csv",
    "text",
    "avro",
    # Database formats (requires JDBC drivers)
    "jdbc",
    "oracle",
    "mysql",
    "postgresql",
    "mssql",
    "db2",
    # Other formats (requires dependencies)
    "delta",  # <-- Requires: `io.delta:delta-core` dependency and `delata-spark` package
    "xml",  # <-- Requires: `com.databricks:spark-xml` dependency and `spark-xml` package
    "excel",  # <-- Requires: `com.crealytics:spark-excel` dependency and `spark-excel` package
    "hive",  # <-- Requires: Hive support
    "mongodb",  # <-- Requires: `org.mongodb.spark:mongo-spark-connector` dependency and `mongo-spark-connector` package
    "cassandra",  # <-- Requires: `com.datastax.spark:spark-cassandra-connector` dependency and `spark-cassandra-connector` package
    "elasticsearch",  # <-- Requires: `org.elasticsearch:elasticsearch-hadoop` dependency and `elasticsearch-hadoop` package
]
"""
The valid formats that can be used to read/write data in Spark.

PySpark's built-in data source formats:
- `parquet`
- `orc`
- `json`
- `csv`
- `text`
- `avro`

Database formats (with proper JDBC drivers):
- `jdbc`
- `oracle`
- `mysql`
- `postgresql`
- `mssql`
- `db2`

Other formats with additional dependencies:
- `delta` (requires: `io.delta:delta-core` dependency and `delata-spark` package)
- `xml` (requires: `com.databricks:spark-xml` dependency and `spark-xml` package)
- `excel` (requires: `com.crealytics:spark-excel` dependency and `spark-excel` package)
- `hive` (requires: Hive support)
- `mongodb` (requires: `org.mongodb.spark:mongo-spark-connector` dependency and `mongo-spark-connector` package)
- `cassandra` (requires: `com.datastax.spark:spark-cassandra-connector` dependency and `spark-cassandra-connector` package)
- `elasticsearch` (requires: `org.elasticsearch:elasticsearch-hadoop` dependency and `elasticsearch-hadoop` package)
"""
VALID_SPARK_FORMATS: str_tuple = get_args(SPARK_FORMATS)
VALID_SPARK_FORMATS.__doc__ = SPARK_FORMATS.__doc__


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Path functions                                                        ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Read                                                                     ####
# ---------------------------------------------------------------------------- #


@typechecked
def read_from_path(
    spark_session: SparkSession,
    name: str,
    path: str,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    read_options: Optional[str_dict] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Read an object from a given `path` in to memory as a `pyspark` dataframe.

    Params:
        spark_session (SparkSession):
            The Spark session to use for the reading.
        name (str):
            The name of the table to read in.
        path (str):
            The path from which it will be read.
        data_format (Optional[SPARK_FORMATS], optional):
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

    # Set default options ----
    read_options: str_dict = read_options or dict()
    data_format: str = data_format or "parquet"
    load_path: str = f"{path}{'/' if not path.endswith('/') else ''}{name}"

    # Initialise reader (including data format) ----
    reader: DataFrameReader = spark_session.read.format(data_format)

    # Add options (if exists) ----
    if read_options:
        reader.options(**read_options)

    # Load DataFrame ----
    return reader.load(load_path)


## --------------------------------------------------------------------------- #
##  Write                                                                   ####
## --------------------------------------------------------------------------- #


@typechecked
def write_to_path(
    data_frame: psDataFrame,
    name: str,
    path: str,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    mode: Optional[str] = None,
    write_options: Optional[str_dict] = None,
    partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        For a given `table`, write it out to a specified `path` with name `name` and format `format`.

    Params:
        data_frame (psDataFrame):
            The DataFrame to be written. Must be a valid `pyspark` DataFrame ([`pyspark.sql.DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)).
        name (str):
            The name of the table where it will be written.
        path (str):
            The path location for where to save the table.
        data_format (Optional[SPARK_FORMATS], optional):
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
        ...     data_frame=df,
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
        ...     data_frame=df,
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

    # Set default options ----
    write_options: str_dict = write_options or dict()
    data_format: str = data_format or "parquet"
    write_path: str = f"{path}{'/' if not path.endswith('/') else ''}{name}"

    # Initialise writer (including data format) ----
    writer: DataFrameWriter = data_frame.write.mode(mode).format(data_format)

    # Add options (if exists) ----
    if write_options:
        writer.options(**write_options)

    # Add partition (if exists) ----
    if partition_cols is not None:
        partition_cols = [partition_cols] if is_type(partition_cols, str) else partition_cols
        writer = writer.partitionBy(list(partition_cols))

    # Write table ----
    writer.save(write_path)


## --------------------------------------------------------------------------- #
##  Transfer                                                                ####
## --------------------------------------------------------------------------- #


@typechecked
def transfer_table_by_path(
    spark_session: SparkSession,
    from_table_path: str,
    from_table_name: str,
    to_table_path: str,
    to_table_name: str,
    from_table_format: Optional[SPARK_FORMATS] = "parquet",
    from_table_options: Optional[str_dict] = None,
    to_table_format: Optional[SPARK_FORMATS] = "parquet",
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
        to_table_path (str):
            The location where to save the table to.
        to_table_name (str):
            The name of the table where it will be saved.
        from_table_format (Optional[SPARK_FORMATS], optional):
            The format of the data at the reading location.
        to_table_format (Optional[SPARK_FORMATS], optional):
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
        >>> transfer_table_by_path(
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
        >>> transfer_table_by_path(
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
        >>> transfer_table_by_path(
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

    # Read from source ----
    from_table: psDataFrame = read_from_path(
        name=from_table_name,
        path=from_table_path,
        spark_session=spark_session,
        data_format=from_table_format,
        read_options=from_table_options,
    )

    # Write to target ----
    write_to_path(
        data_frame=from_table,
        name=to_table_name,
        path=to_table_path,
        data_format=to_table_format,
        mode=to_table_mode,
        write_options=to_table_options,
        partition_cols=to_table_partition_cols,
    )


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Table functions                                                       ####
#                                                                              #
# ---------------------------------------------------------------------------- #


def _validate_table_name(table: str) -> None:
    if "/" in table:
        raise ValidationError(f"Invalid table. Cannot contain `/`: `{table}`")
    if len(table.split(".")) != 2:
        raise ValidationError(
            f"Invalid table. Should be in the format `schema.table`: {table}"
        )


## --------------------------------------------------------------------------- #
##  Read                                                                    ####
## --------------------------------------------------------------------------- #


@typechecked
def read_from_table(
    spark_session: SparkSession,
    name: str,
    schema: Optional[str] = None,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    read_options: Optional[str_dict] = None,
) -> psDataFrame:

    # Set default options ----
    data_format: str = data_format or "parquet"
    table: str = name if not schema else f"{schema}.{name}"

    # Validate that `table` is in the correct format ----
    _validate_table_name(table)

    # Initialise reader (including data format) ----
    reader: DataFrameReader = spark_session.read.format(data_format)

    # Add options (if exists) ----
    if read_options:
        reader.options(**read_options)

    # Load DataFrame ----
    return reader.table(table)


## --------------------------------------------------------------------------- #
##  Write                                                                   ####
## --------------------------------------------------------------------------- #


@typechecked
def write_to_table(
    data_frame: psDataFrame,
    name: str,
    schema: Optional[str] = None,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    mode: Optional[str] = None,
    write_options: Optional[str_dict] = None,
    partition_cols: Optional[str_collection] = None,
) -> None:

    # Set default options ----
    write_options: str_dict = write_options or dict()
    data_format: str = data_format or "parquet"
    table: str = name if not schema else f"{schema}.{name}"

    # Validate that `table` is in the correct format ----
    _validate_table_name(table)

    # Initialise writer (including data format) ----
    writer: DataFrameWriter = data_frame.write.mode(mode).format(data_format)

    # Add options (if exists) ----
    if write_options:
        writer.options(**write_options)

    # Add partition (if exists) ----
    if partition_cols is not None:
        partition_cols = [partition_cols] if is_type(partition_cols, str) else partition_cols
        writer = writer.partitionBy(list(partition_cols))

    # Write table ----
    writer.saveAsTable(table)


## --------------------------------------------------------------------------- #
##  Transfer                                                                ####
## --------------------------------------------------------------------------- #


@typechecked
def transfer_table_by_table(
    spark_session: SparkSession,
    from_table_name: str,
    to_table_name: str,
    from_table_schema: Optional[str] = None,
    from_table_format: Optional[SPARK_FORMATS] = "parquet",
    from_table_options: Optional[str_dict] = None,
    to_table_schema: Optional[str] = None,
    to_table_format: Optional[SPARK_FORMATS] = "parquet",
    to_table_mode: Optional[str] = None,
    to_table_options: Optional[str_dict] = None,
    to_table_partition_cols: Optional[str_collection] = None,
) -> None:

    # Read from source ----
    source_table: psDataFrame = read_from_table(
        name=from_table_name,
        schema=from_table_schema,
        spark_session=spark_session,
        data_format=from_table_format,
        read_options=from_table_options,
    )

    # Write to target ----
    write_to_table(
        data_frame=source_table,
        name=to_table_name,
        schema=to_table_schema,
        data_format=to_table_format,
        mode=to_table_mode,
        write_options=to_table_options,
        partition_cols=to_table_partition_cols,
    )
