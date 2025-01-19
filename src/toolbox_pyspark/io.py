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
from toolbox_python.collection_types import (
    str_collection,
    str_dict,
    str_list,
    str_tuple,
)
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.utils.exceptions import ValidationError


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "SPARK_FORMATS",
    "VALID_SPARK_FORMATS",
    "WRITE_MODES",
    "VALID_WRITE_MODES",
    "read_from_path",
    "write_to_path",
    "transfer_by_path",
    "read_from_table",
    "write_to_table",
    "transfer_by_table",
    "read",
    "write",
    "transfer",
    "load_from_path",
    "save_to_path",
    "load_from_table",
    "save_to_table",
    "load",
    "save",
]


## --------------------------------------------------------------------------- #
##  Constants                                                               ####
## --------------------------------------------------------------------------- #


### Data formats ----
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


### Write modes ----
WRITE_MODES = Literal["append", "overwrite", "ignore", "error", "errorifexists"]
"""
The valid modes you can use for writing data frames:

- `append`
- `overwrite`
- `ignore`
- `error`
- `errorifexists`
"""

VALID_WRITE_MODES: str_tuple = get_args(WRITE_MODES)
"""
The valid modes you can use for writing data frames:

- `append`
- `overwrite`
- `ignore`
- `error`
- `errorifexists`
"""


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

        ```{.py .python linenums="1" title="Example 3: Invalid Path"}
        >>> df_invalid_path = read_from_path(
        ...     name="invalid_table.csv",
        ...     path="./invalid_path",
        ...     spark_session=spark,
        ...     data_format="csv",
        ...     options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.load.
        ```
        !!! failure "Conclusion: Failed to read from invalid path."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid Format"}
        >>> df_invalid_format = read_from_path(
        ...     name="table.csv",
        ...     path="./test",
        ...     spark_session=spark,
        ...     data_format="invalid_format",
        ...     options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.load.
        ```
        !!! failure "Conclusion: Failed to read due to invalid format."
        </div>

    ??? tip "See Also"
        - [`load_from_path`][toolbox_pyspark.io.load_from_path]
        - [`read`][toolbox_pyspark.io.read]
        - [`load`][toolbox_pyspark.io.load]
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
    mode: Optional[WRITE_MODES] = None,
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
        mode (Optional[WRITE_MODES], optional):
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

        ```{.py .python linenums="1" title="Example 3: Invalid Path"}
        >>> write_to_path(
        ...     data_frame=df,
        ...     name="df.csv",
        ...     path="./invalid_path",
        ...     data_format="csv",
        ...     mode="overwrite",
        ...     options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.save.
        ```
        !!! failure "Conclusion: Failed to write to invalid path."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid Format"}
        >>> write_to_path(
        ...     data_frame=df,
        ...     name="df.csv",
        ...     path="./test",
        ...     data_format="invalid_format",
        ...     mode="overwrite",
        ...     options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.save.
        ```
        !!! failure "Conclusion: Failed to write due to invalid format."
        </div>

    ??? tip "See Also"
        - [`save_to_path`][toolbox_pyspark.io.save_to_path]
        - [`write`][toolbox_pyspark.io.write]
        - [`save`][toolbox_pyspark.io.save]
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
def transfer_by_path(
    spark_session: SparkSession,
    from_table_path: str,
    from_table_name: str,
    to_table_path: str,
    to_table_name: str,
    from_table_format: Optional[SPARK_FORMATS] = "parquet",
    from_table_options: Optional[str_dict] = None,
    to_table_format: Optional[SPARK_FORMATS] = "parquet",
    to_table_mode: Optional[WRITE_MODES] = None,
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
        to_table_mode (Optional[WRITE_MODES], optional):
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
        >>> from toolbox_pyspark.io import transfer_by_path
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
        >>> transfer_by_path(
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
        >>> transfer_by_path(
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
        >>> transfer_by_path(
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

        ```{.py .python linenums="1" title="Example 4: Invalid Source Path"}
        >>> transfer_by_path(
        ...     spark_session=spark,
        ...     from_table_path="./invalid_path",
        ...     from_table_name="table.csv",
        ...     from_table_format="csv",
        ...     to_table_path="./other",
        ...     to_table_name="table.csv",
        ...     to_table_format="csv",
        ...     from_table_options={"header": "true"},
        ...     to_table_mode="overwrite",
        ...     to_table_options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.load.
        ```
        !!! failure "Conclusion: Failed to transfer due to invalid source path."
        </div>

        ```{.py .python linenums="1" title="Example 5: Invalid Target Format"}
        >>> transfer_by_path(
        ...     spark_session=spark,
        ...     from_table_path="./test",
        ...     from_table_name="table.csv",
        ...     from_table_format="csv",
        ...     to_table_path="./other",
        ...     to_table_name="table.csv",
        ...     to_table_format="invalid_format",
        ...     from_table_options={"header": "true"},
        ...     to_table_mode="overwrite",
        ...     to_table_options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.save.
        ```
        !!! failure "Conclusion: Failed to transfer due to invalid target format."
        </div>

    ??? tip "See Also"
        - [`transfer`][toolbox_pyspark.io.transfer]
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
    """
    !!! note "Summary"
        Read a table from a given `schema` and `name` into memory as a `pyspark` dataframe.

    ???+ abstract "Details"
        - If `schema` is `#!py None`, then we would expect the `name` to contain both the schema and the table name in the same. Like: `schema.name`, for example `production.orders`.
        - Else, if `schema` is not `#! None`, then we would expect the `schema` to (quite logically) contain the name of the schema, and the `name` to contain the name of the table.

    Params:
        spark_session (SparkSession):
            The Spark session to use for the reading.
        name (str):
            The name of the table to read in.
        schema (Optional[str], optional):
            The schema of the table to read in.<br>
            Defaults to `#!py None`.
        data_format (Optional[SPARK_FORMATS], optional):
            The format of the table.<br>
            Defaults to `#!py "parquet"`.
        read_options (Dict[str, str], optional):
            Any additional options to parse to the Spark reader.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#!py dict()`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValidationError:
            If `name` contains `/`, or is structured with three elements like: `source.schema.table`.

    Returns:
        (psDataFrame):
            The loaded dataframe.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import read_from_table
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
        >>> df.to_parquet("./test/table.parquet")
        >>> spark.read.parquet("./test/table.parquet").createOrReplaceTempView("test_table")
        ```

        ```{.py .python linenums="1" title="Example 1: Read Table"}
        >>> df_table = read_from_table(
        ...     name="test_table",
        ...     spark_session=spark,
        ...     data_format="parquet",
        ... )
        >>>
        >>> df_table.show()
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
        !!! success "Conclusion: Successfully read table."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid table structure"}
        >>> df_table = read_from_table(
        ...     name="schema.test_table",
        ...     schema="source",
        ...     spark_session=spark,
        ...     data_format="parquet",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.test_table`.
        ```
        !!! failure "Conclusion: Failed to write to table due to invalid table structure."
        </div>

    ??? tip "See Also"
        - [`save_to_table`][toolbox_pyspark.io.save_to_table]
        - [`write`][toolbox_pyspark.io.write]
        - [`save`][toolbox_pyspark.io.save]
    """

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
    mode: Optional[WRITE_MODES] = None,
    write_options: Optional[str_dict] = None,
    partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        For a given `data_frame`, write it out to a specified `schema` and `name` with format `data_format`.

    ???+ abstract "Details"
        - If `schema` is `#!py None`, then we would expect the `name` to contain both the schema and the table name in the same. Like: `schema.name`, for example `production.orders`.
        - Else, if `schema` is not `#! None`, then we would expect the `schema` to (quite logically) contain the name of the schema, and the `name` to contain the name of the table.

    Params:
        data_frame (psDataFrame):
            The DataFrame to be written. Must be a valid `pyspark` DataFrame ([`pyspark.sql.DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)).
        name (str):
            The name of the table where it will be written.
        schema (Optional[str], optional):
            The schema of the table where it will be written.<br>
            Defaults to `#!py None`.
        data_format (Optional[SPARK_FORMATS], optional):
            The format that the `data_frame` will be written to.<br>
            Defaults to `#!py "parquet"`.
        mode (Optional[WRITE_MODES], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        write_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            Like, for example:

            - If you are writing to a Delta object, and wanted to overwrite the schema: `#!py {"overwriteSchema": "true"}`.
            - If you're writing to a CSV file, and wanted to specify the header row: `#!py {"header": "true"}`.

            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#!py dict()`.
        partition_cols (Optional[Union[str_collection, str]], optional):
            The column(s) that the table should partition by.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValidationError:
            If `name` contains `/`, or is structured with three elements like: `source.schema.table`.

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
        >>> from toolbox_pyspark.io import write_to_table
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

        ```{.py .python linenums="1" title="Example 1: Write to Table"}
        >>> write_to_table(
        ...     data_frame=df,
        ...     name="test_table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ... )
        >>>
        >>> table_exists(
        ...     name="test_table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     spark_session=df.sparkSession,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully written to table."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid table structure"}
        >>> write_to_table(
        ...     data_frame=df,
        ...     name="schema.test_table",
        ...     schema="source",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.test_table`.
        ```
        !!! failure "Conclusion: Failed to write to table due to invalid table structure."
        </div>

    ??? tip "See Also"
        - [`save_to_table`][toolbox_pyspark.io.save_to_table]
        - [`write`][toolbox_pyspark.io.write]
        - [`save`][toolbox_pyspark.io.save]
    """

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
def transfer_by_table(
    spark_session: SparkSession,
    from_table_name: str,
    to_table_name: str,
    from_table_schema: Optional[str] = None,
    from_table_format: Optional[SPARK_FORMATS] = "parquet",
    from_table_options: Optional[str_dict] = None,
    to_table_schema: Optional[str] = None,
    to_table_format: Optional[SPARK_FORMATS] = "parquet",
    to_table_mode: Optional[WRITE_MODES] = None,
    to_table_options: Optional[str_dict] = None,
    to_table_partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        Copy a table from one schema and name to another schema and name.

    ???+ abstract "Details"
        This is a blind transfer. There is no validation, no alteration, no adjustments made at all. Simply read directly from one table and move immediately to another table straight away.

    Params:
        spark_session (SparkSession):
            The spark session to use for the transfer. Necessary in order to instantiate the reading process.
        from_table_name (str):
            The name of the table to be read.
        to_table_name (str):
            The name of the table where it will be saved.
        from_table_schema (Optional[str], optional):
            The schema of the table to be read.<br>
            Defaults to `#!py None`.
        from_table_format (Optional[SPARK_FORMATS], optional):
            The format of the data at the reading location.<br>
            Defaults to `#!py "parquet"`.
        from_table_options (Dict[str, str], optional):
            Any additional options to parse to the Spark reader.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#!py dict()`.
        to_table_schema (Optional[str], optional):
            The schema of the table where it will be saved.<br>
            Defaults to `#!py None`.
        to_table_format (Optional[SPARK_FORMATS], optional):
            The format of the saved table.<br>
            Defaults to `#!py "parquet"`.
        to_table_mode (Optional[WRITE_MODES], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        to_table_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#!py dict()`.
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
        >>> from toolbox_pyspark.io import transfer_by_table
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
        ...         "c": [1, 1, 1, 1],
        ...         "d": ["2", "2", "2", "2"],
        ...     }
        ... )
        >>> df.to_parquet("./test/table.parquet")
        >>> spark.read.parquet("./test/table.parquet").createOrReplaceTempView("test_table")
        ```

        ```{.py .python linenums="1" title="Example 1: Transfer Table"}
        >>> transfer_by_table(
        ...     spark_session=spark,
        ...     from_table_name="test_table",
        ...     from_table_schema="default",
        ...     from_table_format="parquet",
        ...     to_table_name="new_table",
        ...     to_table_schema="default",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ... )
        >>>
        >>> table_exists(
        ...     name="new_table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred table."
        </div>

        ```{.py .python linenums="1" title="Example 2: Invalid table structure"}
        >>> transfer_by_table(
        ...     spark_session=spark,
        ...     from_table_name="schema.test_table",
        ...     from_table_schema="source",
        ...     from_table_format="parquet",
        ...     to_table_name="new_table",
        ...     to_table_schema="default",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.test_table`.
        ```
        !!! failure "Conclusion: Failed to transfer table due to invalid table structure."
        </div>

    ??? tip "See Also"
        - [`transfer`][toolbox_pyspark.io.transfer]
    """

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


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Combined Functions                                                    ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Read                                                                    ####
## --------------------------------------------------------------------------- #


@typechecked
def read(
    spark_session: SparkSession,
    name: str,
    method: Literal["table", "path"],
    path: Optional[str] = None,
    schema: Optional[str] = None,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    read_options: Optional[str_dict] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Read a table or file from a given `path` or `schema` and `name` into memory as a `pyspark` dataframe.

    ???+ abstract "Details"
        This function serves as a unified interface for reading data into a `pyspark` dataframe. Depending on the `method` parameter, it will either read from a file path or a table.

        - If `method` is `#!py "path"`, the function will use the `read_from_path` function to read the data from the specified `path` and `name`.
        - If `method` is `#!py "table"`, the function will use the `read_from_table` function to read the data from the specified `schema` and `name`.

    Params:
        spark_session (SparkSession):
            The Spark session to use for the reading.
        name (str):
            The name of the table or file to read in.
        method (Literal["table", "path"]):
            The method to use for reading the data. Either `#!py "table"` or `#!py "path"`.
        path (Optional[str], optional):
            The path from which the file will be read. Required if `method` is `#!py "path"`.<br>
            Defaults to `#!py None`.
        schema (Optional[str], optional):
            The schema of the table to read in. Required if `method` is `#!py "table"`.<br>
            Defaults to `#!py None`.
        data_format (Optional[SPARK_FORMATS], optional):
            The format of the data.<br>
            Defaults to `#!py "parquet"`.
        read_options (Dict[str, str], optional):
            Any additional options to parse to the Spark reader.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#!py dict()`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValidationError:
            If `name` contains `/`, or is structured with three elements like: `source.schema.table`.

    Returns:
        (psDataFrame):
            The loaded dataframe.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import read
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
        >>> df.to_csv("./test/table.csv")
        >>> df.to_parquet("./test/table.parquet")
        >>> spark.read.parquet("./test/table.parquet").createOrReplaceTempView("test_table")
        ```

        ```{.py .python linenums="1" title="Example 1: Read from Path"}
        >>> df_path = read(
        ...     spark_session=spark,
        ...     name="table.csv",
        ...     method="path",
        ...     path="./test",
        ...     data_format="csv",
        ...     read_options={"header": "true"},
        ... )
        >>>
        >>> df_path.show()
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
        !!! success "Conclusion: Successfully read from path."
        </div>

        ```{.py .python linenums="1" title="Example 2: Read from Table"}
        >>> df_table = read(
        ...     spark_session=spark,
        ...     name="test_table",
        ...     method="table",
        ...     schema="default",
        ...     data_format="parquet",
        ... )
        >>>
        >>> df_table.show()
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
        !!! success "Conclusion: Successfully read from table."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid Path"}
        >>> df_invalid_path = read(
        ...     spark_session=spark,
        ...     name="invalid_table.csv",
        ...     method="path",
        ...     path="./invalid_path",
        ...     data_format="csv",
        ...     read_options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.load.
        ```
        !!! failure "Conclusion: Failed to read from invalid path."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid Table Structure"}
        >>> df_invalid_table = read(
        ...     spark_session=spark,
        ...     name="schema.invalid_table",
        ...     method="table",
        ...     schema="source",
        ...     data_format="parquet",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.invalid_table`.
        ```
        !!! failure "Conclusion: Failed to read from invalid table structure."
        </div>

    ??? tip "See Also"
        - [`read_from_path`][toolbox_pyspark.io.read_from_path]
        - [`read_from_table`][toolbox_pyspark.io.read_from_table]
        - [`load`][toolbox_pyspark.io.load]
    """

    if method == "table":
        return read_from_table(
            spark_session=spark_session,
            name=name,
            schema=schema,
            data_format=data_format,
            read_options=read_options,
        )
    if method == "path":
        return read_from_path(
            spark_session=spark_session,
            name=name,
            path=path,
            data_format=data_format,
            read_options=read_options,
        )


## --------------------------------------------------------------------------- #
##  Write                                                                   ####
## --------------------------------------------------------------------------- #


@typechecked
def write(
    data_frame: psDataFrame,
    name: str,
    method: Literal["table", "path"],
    path: Optional[str] = None,
    schema: Optional[str] = None,
    data_format: Optional[SPARK_FORMATS] = "parquet",
    mode: Optional[WRITE_MODES] = None,
    write_options: Optional[str_dict] = None,
    partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        Write a dataframe to a specified `path` or `schema` and `name` with format `data_format`.

    ???+ abstract "Details"
        This function serves as a unified interface for writing data from a `pyspark` dataframe. Depending on the `method` parameter, it will either write to a file path or a table.

        - If `method` is `#!py "path"`, the function will use the `write_to_path` function to write the data to the specified `path` and `name`.
        - If `method` is `#!py "table"`, the function will use the `write_to_table` function to write the data to the specified `schema` and `name`.

    Params:
        data_frame (psDataFrame):
            The DataFrame to be written. Must be a valid `pyspark` DataFrame ([`pyspark.sql.DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)).
        name (str):
            The name of the table or file where it will be written.
        method (Literal["table", "path"]):
            The method to use for writing the data. Either `#!py "table"` or `#!py "path"`.
        path (Optional[str], optional):
            The path location for where to save the table. Required if `method` is `#!py "path"`.<br>
            Defaults to `#!py None`.
        schema (Optional[str], optional):
            The schema of the table where it will be written. Required if `method` is `#!py "table"`.<br>
            Defaults to `#!py None`.
        data_format (Optional[SPARK_FORMATS], optional):
            The format that the `data_frame` will be written to.<br>
            Defaults to `#!py "parquet"`.
        mode (Optional[WRITE_MODES], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        write_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            Like, for example:

            - If you are writing to a Delta object, and wanted to overwrite the schema: `#!py {"overwriteSchema": "true"}`.
            - If you're writing to a CSV file, and wanted to specify the header row: `#!py {"header": "true"}`.

            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#!py dict()`.
        partition_cols (Optional[Union[str_collection, str]], optional):
            The column(s) that the table should partition by.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValidationError:
            If `name` contains `/`, or is structured with three elements like: `source.schema.table`.

    Returns:
        (type(None)):
            Nothing is returned.

    ???+ tip "Note"
        You know that this function is successful if the table or file exists at the specified location, and there are no errors thrown.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import write
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

        ```{.py .python linenums="1" title="Example 1: Write to Path"}
        >>> write(
        ...     data_frame=df,
        ...     name="df.csv",
        ...     method="path",
        ...     path="./test",
        ...     data_format="csv",
        ...     mode="overwrite",
        ...     write_options={"header": "true"},
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
        !!! success "Conclusion: Successfully written to path."
        </div>

        ```{.py .python linenums="1" title="Example 2: Write to Table"}
        >>> write(
        ...     data_frame=df,
        ...     name="test_table",
        ...     method="table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ... )
        >>>
        >>> table_exists(
        ...     name="test_table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     spark_session=df.sparkSession,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully written to table."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid Path"}
        >>> write(
        ...     data_frame=df,
        ...     name="df.csv",
        ...     method="path",
        ...     path="./invalid_path",
        ...     data_format="csv",
        ...     mode="overwrite",
        ...     write_options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.save.
        ```
        !!! failure "Conclusion: Failed to write to invalid path."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid Table Structure"}
        >>> write(
        ...     data_frame=df,
        ...     name="schema.test_table",
        ...     method="table",
        ...     schema="source",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.test_table`.
        ```
        !!! failure "Conclusion: Failed to write to table due to invalid table structure."
        </div>

    ??? tip "See Also"
        - [`write_to_path`][toolbox_pyspark.io.write_to_path]
        - [`write_to_table`][toolbox_pyspark.io.write_to_table]
        - [`save`][toolbox_pyspark.io.save]
    """

    if method == "table":
        write_to_table(
            data_frame=data_frame,
            name=name,
            schema=schema,
            data_format=data_format,
            mode=mode,
            write_options=write_options,
            partition_cols=partition_cols,
        )
    if method == "path":
        write_to_path(
            data_frame=data_frame,
            name=name,
            path=path,
            data_format=data_format,
            mode=mode,
            write_options=write_options,
            partition_cols=partition_cols,
        )


## --------------------------------------------------------------------------- #
##  Transfer                                                                ####
## --------------------------------------------------------------------------- #


@typechecked
def transfer(
    spark_session: SparkSession,
    from_table_name: str,
    to_table_name: str,
    method: Literal["table", "path"],
    from_table_path: Optional[str] = None,
    from_table_schema: Optional[str] = None,
    from_table_format: Optional[SPARK_FORMATS] = "parquet",
    from_table_options: Optional[str_dict] = None,
    to_table_path: Optional[str] = None,
    to_table_schema: Optional[str] = None,
    to_table_format: Optional[SPARK_FORMATS] = "parquet",
    to_table_mode: Optional[WRITE_MODES] = None,
    to_table_options: Optional[str_dict] = None,
    to_partition_cols: Optional[str_collection] = None,
) -> None:
    """
    !!! note "Summary"
        Transfer a table or file from one location to another.

    ???+ abstract "Details"
        This function serves as a unified interface for transferring data from one location to another. Depending on the `method` parameter, it will either transfer from a file path or a table.

        - If `method` is `#!py "path"`, the function will use the `transfer_by_path` function to transfer the data from the specified `from_table_path` and `from_table_name` to the specified `to_table_path` and `to_table_name`.
        - If `method` is `#!py "table"`, the function will use the `transfer_by_table` function to transfer the data from the specified `from_table_schema` and `from_table_name` to the specified `to_table_schema` and `to_table_name`.

    Params:
        spark_session (SparkSession):
            The Spark session to use for the transfer.
        from_table_name (str):
            The name of the table or file to be transferred.
        to_table_name (str):
            The name of the table or file where it will be transferred.
        method (Literal["table", "path"]):
            The method to use for transferring the data. Either `#!py "table"` or `#!py "path"`.
        from_table_path (Optional[str], optional):
            The path from which the file will be transferred. Required if `method` is `#!py "path"`.<br>
            Defaults to `#!py None`.
        from_table_schema (Optional[str], optional):
            The schema of the table to be transferred. Required if `method` is `#!py "table"`.<br>
            Defaults to `#!py None`.
        from_table_format (Optional[SPARK_FORMATS], optional):
            The format of the data at the source location.<br>
            Defaults to `#!py "parquet"`.
        from_table_options (Dict[str, str], optional):
            Any additional options to parse to the Spark reader.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameReader.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.options.html).<br>
            Defaults to `#!py dict()`.
        to_table_path (Optional[str], optional):
            The path location for where to save the table. Required if `method` is `#!py "path"`.<br>
            Defaults to `#!py None`.
        to_table_schema (Optional[str], optional):
            The schema of the table where it will be saved. Required if `method` is `#!py "table"`.<br>
            Defaults to `#!py None`.
        to_table_format (Optional[SPARK_FORMATS], optional):
            The format of the saved table.<br>
            Defaults to `#!py "parquet"`.
        to_table_mode (Optional[WRITE_MODES], optional):
            The behaviour for when the data already exists.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.mode`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html).<br>
            Defaults to `#!py None`.
        to_table_options (Dict[str, str], optional):
            Any additional settings to parse to the writer class.<br>
            For more info, check the `pyspark` docs: [`pyspark.sql.DataFrameWriter.options`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html).<br>
            Defaults to `#!py dict()`.
        to_partition_cols (Optional[Union[str_collection, str]], optional):
            The column(s) that the table should partition by.<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        ValidationError:
            If `from_table_name` or `to_table_name` contains `/`, or is structured with three elements like: `source.schema.table`.

    Returns:
        (type(None)):
            Nothing is returned.

    ???+ tip "Note"
        You know that this function is successful if the table or file exists at the specified location, and there are no errors thrown.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.io import transfer
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
        ...         "c": [1, 1, 1, 1],
        ...         "d": ["2", "2", "2", "2"],
        ...     }
        ... )
        >>> df.to_csv("./test/table.csv")
        >>> df.to_parquet("./test/table.parquet")
        >>> spark.read.parquet("./test/table.parquet").createOrReplaceTempView("test_table")
        ```

        ```{.py .python linenums="1" title="Example 1: Transfer from Path"}
        >>> transfer(
        ...     spark_session=spark,
        ...     method="path",
        ...     from_table_name="table.csv",
        ...     from_table_path="./test",
        ...     from_table_format="csv",
        ...     from_table_options={"header": "true"},
        ...     to_table_name="new_table.csv",
        ...     to_table_path="./other",
        ...     to_table_format="csv",
        ...     to_table_mode="overwrite",
        ...     to_table_options={"header": "true"},
        ... )
        >>>
        >>> table_exists(
        ...     name="new_table.csv",
        ...     path="./other",
        ...     data_format="csv",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred from path."
        </div>

        ```{.py .python linenums="1" title="Example 2: Transfer from Table"}
        >>> transfer(
        ...     spark_session=spark,
        ...     method="table",
        ...     from_table_name="test_table",
        ...     from_table_schema="default",
        ...     from_table_format="parquet",
        ...     to_table_name="new_table",
        ...     to_table_schema="default",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ... )
        >>>
        >>> table_exists(
        ...     name="new_table",
        ...     schema="default",
        ...     data_format="parquet",
        ...     spark_session=spark,
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Successfully transferred from table."
        </div>

        ```{.py .python linenums="1" title="Example 3: Invalid Path"}
        >>> transfer(
        ...     spark_session=spark,
        ...     method="path",
        ...     from_table_name="table.csv",
        ...     from_table_path="./invalid_path",
        ...     from_table_format="csv",
        ...     from_table_options={"header": "true"},
        ...     to_table_name="new_table.csv",
        ...     to_table_path="./other",
        ...     to_table_format="csv",
        ...     to_table_mode="overwrite",
        ...     to_table_options={"header": "true"},
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Py4JJavaError: An error occurred while calling o45.load.
        ```
        !!! failure "Conclusion: Failed to transfer from invalid path."
        </div>

        ```{.py .python linenums="1" title="Example 4: Invalid Table Structure"}
        >>> transfer(
        ...     spark_session=spark,
        ...     method="table",
        ...     from_table_name="schema.test_table",
        ...     from_table_schema="source",
        ...     from_table_format="parquet",
        ...     to_table_name="new_table",
        ...     to_table_schema="default",
        ...     to_table_format="parquet",
        ...     to_table_mode="overwrite",
        ... )
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        Invalid table. Should be in the format `schema.table`: `source.schema.test_table`.
        ```
        !!! failure "Conclusion: Failed to transfer from invalid table structure."
        </div>

    ??? tip "See Also"
        - [`transfer_by_path`][toolbox_pyspark.io.transfer_by_path]
        - [`transfer_by_table`][toolbox_pyspark.io.transfer_by_table]
    """

    if method == "table":
        transfer_by_table(
            spark_session=spark_session,
            from_table_name=from_table_name,
            to_table_name=to_table_name,
            from_table_schema=from_table_schema,
            from_table_format=from_table_format,
            from_table_options=from_table_options,
            to_table_schema=to_table_schema,
            to_table_format=to_table_format,
            to_table_mode=to_table_mode,
            to_table_options=to_table_options,
            to_table_partition_cols=to_partition_cols,
        )
    if method == "path":
        transfer_by_path(
            spark_session=spark_session,
            from_table_path=from_table_path,
            from_table_name=from_table_name,
            from_table_format=from_table_format,
            to_table_path=to_table_path,
            to_table_name=to_table_name,
            to_table_format=to_table_format,
            from_table_options=from_table_options,
            to_table_mode=to_table_mode,
            to_table_options=to_table_options,
            to_table_partition_cols=to_partition_cols,
        )


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Aliases                                                               ####
#                                                                              #
# ---------------------------------------------------------------------------- #

load_from_path = read_from_path
save_to_path = write_to_path
load_from_table = read_from_table
save_to_table = write_to_table
load = read
save = write
