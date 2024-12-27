# ============================================================================ #
#                                                                              #
#     Title   : Delta                                                          #
#     Purpose : For various processes related to Delta Lake tables.            #
#               Including optimising tables, merging tables, retrieving table  #
#               history, and transferring between.                             #
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
    The `delta` module is for various processes related to Delta Lake tables. Including optimising tables, merging tables, retrieving table history, and transferring between locations.
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
import os
from typing import Literal, Optional, Type, Union

# ## Python Third Party Imports ----
import pandas as pd
import pyspark.sql.functions as F
from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from stamina import retry
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_dict, str_list
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.checks import assert_columns_exists
from toolbox_pyspark.columns import get_columns
from toolbox_pyspark.dimensions import get_dims
from toolbox_pyspark.io import read_from_path


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "load_table",
    "count_rows",
    "get_history",
    "is_partitioned",
    "get_partition_columns",
    "optimise_table",
    "retry_optimise_table",
    "merge_spark_to_delta",
    "merge_delta_to_delta",
    "retry_merge_spark_to_delta",
    "DeltaLoader",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Table processes                                                          ####
# ---------------------------------------------------------------------------- #


@typechecked
def load_table(
    name: str,
    path: str,
    spark_session: SparkSession,
) -> DeltaTable:
    """
    !!! note "Summary"
        Load a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) from a path.

    ???+ abstract "Details"
        Under the hood, this function simply calls the [`.forPath()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.forPath) method

    Params:
        name (str):
            The name of the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).
        path (str):
            The path where the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) is found.
        spark_session (SparkSession):
            The SparkSession to use for loading the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (DeltaTable):
            The loaded [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    ??? tip "See also"
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable
        - [`DeltaTable.forPath()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.forPath)
    """
    return DeltaTable.forPath(
        sparkSession=spark_session,
        path=f"{path}{'/' if not path.endswith('/') else ''}{name}",
    )


# ---------------------------------------------------------------------------- #
#  Table details                                                            ####
# ---------------------------------------------------------------------------- #


@typechecked
def count_rows(
    table: Union[str, DeltaTable],
    path: Optional[str] = None,
    spark_session: Optional[SparkSession] = None,
) -> int:
    """
    !!! note "Summary"
        Count the number of rows on a given [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    ???+ abstract "Details"
        Under the hood, this function will convert the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) to a Spark [`DataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html) to then execute the [`.count()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html) method.

    Params:
        table (Union[str, DeltaTable]):
            The table to check.<br>
            If it is a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable), then it will immediately use that.<br>
            If it is a `#!py str`, then it will use that as the name of the table from where to load the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) from.
        path (Optional[str], optional):
            If `table` is a `#!py str`, then `path` is mandatory, and is used as the `path` location for where to find the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) to load from.<br>
            Defaults to `#!py None`.
        spark_session (Optional[SparkSession], optional):
            If `table` is `#!py str`, then `spark_session` is mandatory. This is the [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html) to use for loading the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AssertionError:
            If `table` is a `str`, then `path` and `spark_session` cannot be `None`.

    Returns:
        (int):
            The number of rows on `table`.

    ??? tip "See also"
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.toDF()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.toDF)
        - [`pyspark.sql.DataFrame.count()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.count.html)
    """
    if is_type(table, str):
        assert path is not None, "If `table` is a `str`, then `path` cannot be `None`."
        assert (
            spark_session is not None
        ), "If `table` is a `str`, then `spark_session` cannot be `None`."
        table = load_table(name=table, path=path, spark_session=spark_session)
    return table.toDF().count()


@typechecked
def get_history(
    table: Union[str, DeltaTable],
    path: Optional[str] = None,
    spark_session: Optional[SparkSession] = None,
) -> psDataFrame:
    """
    !!! note "Summary"
        Retrieve the transaction history for a given [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    ???+ abstract "Details"
        Under the hood, this function will simply call the [`.history()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.history) method.

    Params:
        table (Union[str, DeltaTable]):
            The table to check.<br>
            If it is a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable), then it will immediately use that.<br>
            If it is a `#!py str`, then it will use that as the name of the table from where to load the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) from.
        path (Optional[str], optional):
            If `table` is a `#!py str`, then `path` is mandatory, and is used as the `path` location for where to find the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) to load from.<br>
            Defaults to `#!py None`.
        spark_session (Optional[SparkSession], optional):
            If `table` is `#!py str`, then `spark_session` is mandatory. This is the [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html) to use for loading the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AssertionError:
            If `table` is a `str`, then `path` and `spark_session` cannot be `None`.

    Returns:
        (psDataFrame):
            The transaction history for a given [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    ??? tip "See also"
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.history()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.history)
    """
    if is_type(table, str):
        assert path is not None, "If `table` is a `str`, then `path` cannot be `None`."
        assert (
            spark_session is not None
        ), "If `table` is a `str`, then `spark_session` cannot be `None`."
        table = load_table(name=table, path=path, spark_session=spark_session)
    return table.history()


@typechecked
def is_partitioned(
    table: Union[str, DeltaTable],
    path: Optional[str] = None,
    spark_session: Optional[SparkSession] = None,
) -> bool:
    """
    !!! note "Summary"
        Check whether a given [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) is partitioned.

    ???+ abstract "Details"
        Under the hood, this function will retrieve the table details and check the `partitionColumns` attribute to determine if the table is partitioned.

    Params:
        table (Union[str, DeltaTable]):
            The table to check.<br>
            If it is a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable), then it will immediately use that.<br>
            If it is a `#!py str`, then it will use that as the name of the table from where to load the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) from.
        path (Optional[str], optional):
            If `table` is a `#!py str`, then `path` is mandatory, and is used as the `path` location for where to find the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) to load from.<br>
            Defaults to `#!py None`.
        spark_session (Optional[SparkSession], optional):
            If `table` is `#!py str`, then `spark_session` is mandatory. This is the [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html) to use for loading the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AssertionError:
            If `table` is a `str`, then `path` and `spark_session` cannot be `None`.

    Returns:
        (bool):
            `#!py True` if the table is partitioned, `#!py False` otherwise.

    ??? tip "See also"
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.detail()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.detail)
    """
    if is_type(table, str):
        assert path is not None, "If `table` is a `str`, then `path` cannot be `None`."
        assert (
            spark_session is not None
        ), "If `table` is a `str`, then `spark_session` cannot be `None`."
        table = load_table(
            name=table,
            path=path,
            spark_session=spark_session,
        )
    return len(table.detail().select("partitionColumns").collect()[0][0]) > 0


@typechecked
def get_partition_columns(
    table: Union[str, DeltaTable],
    path: Optional[str] = None,
    spark_session: Optional[SparkSession] = None,
) -> Optional[str_list]:
    """
    !!! note "Summary"
        Retrieve the partition columns for a given [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).

    ???+ abstract "Details"
        Under the hood, this function will retrieve the table details and return the `partitionColumns` attribute if the table is partitioned.

    Params:
        table (Union[str, DeltaTable]):
            The table to check.<br>
            If it is a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable), then it will immediately use that.<br>
            If it is a `#!py str`, then it will use that as the name of the table from where to load the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) from.
        path (Optional[str], optional):
            If `table` is a `#!py str`, then `path` is mandatory, and is used as the `path` location for where to find the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) to load from.<br>
            Defaults to `#!py None`.
        spark_session (Optional[SparkSession], optional):
            If `table` is `#!py str`, then `spark_session` is mandatory. This is the [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html) to use for loading the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable).<br>
            Defaults to `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AssertionError:
            If `table` is a `str`, then `path` and `spark_session` cannot be `None`.

    Returns:
        (Optional[str_list]):
            The list of partition columns if the table is partitioned, `#!py None` otherwise.

    ??? tip "See also"
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.detail()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.detail)
    """
    if is_type(table, str):
        assert path is not None, "If `table` is a `str`, then `path` cannot be `None`."
        assert (
            spark_session is not None
        ), "If `table` is a `str`, then `spark_session` cannot be `None`."
        table = load_table(name=table, path=path, spark_session=spark_session)
    if is_partitioned(table):
        return table.detail().select("partitionColumns").collect()[0][0]
    else:
        return None


# ---------------------------------------------------------------------------- #
#  Optimisation processes                                                   ####
# ---------------------------------------------------------------------------- #


@typechecked
def _optimise_table_sql(
    table_name: str,
    table_path: str,
    spark_session: SparkSession,
    partition_cols: Optional[str_collection] = None,
    inspect: bool = False,
    return_result: bool = True,
    conditional_where_clause: Optional[str] = None,
) -> Optional[psDataFrame]:

    # Set variables
    if partition_cols is not None:
        partition_cols = [partition_cols] if is_type(partition_cols, str) else partition_cols
        zorder: str = f" ZORDER BY ({', '.join(partition_cols)})"
    else:
        zorder = ""

    where: str = (
        f" WHERE {conditional_where_clause}" if conditional_where_clause is not None else ""
    )

    # Execute queries
    command: str = (
        f"OPTIMIZE delta.`{table_path}{'/' if not table_path.endswith('/') else ''}{table_name}`{where}{zorder}"
    )
    results: psDataFrame = spark_session.sql(command)

    # Return
    if inspect:
        print(command)
    return results if return_result else None


@typechecked
def _optimise_table_api(
    table_name: str,
    table_path: str,
    spark_session: SparkSession,
    partition_cols: Optional[str_collection] = None,
    inspect: Optional[bool] = False,
    return_result: Optional[bool] = True,
    conditional_where_clause: Optional[str] = None,
) -> Optional[psDataFrame]:

    # Set variables
    if partition_cols is not None:
        partition_cols = [partition_cols] if is_type(partition_cols, str) else partition_cols

    # By API
    table_dlt: DeltaTable = load_table(
        name=table_name,
        path=table_path,
        spark_session=spark_session,
    ).alias("dlt")

    # Inspect
    if inspect:
        print(f"{table_name=}")
        print(f"{table_path=}")
        print(f"{partition_cols=}")
        print(f"{conditional_where_clause=}")
        print(f"{return_result=}")
        print(f"dims={get_dims(table_dlt.toDF())}")

    # Begin optimisation
    table_dlt = table_dlt.optimize()

    # Optional WHERE
    if conditional_where_clause is not None:
        # Example: "dlt.editdate >= add_months(current_date(), -6)"
        # Check whether this is actually necessary and/or provides any efficiency gain.
        table_dlt = table_dlt.where(conditional_where_clause)

    # Optional ZORDERBY
    if partition_cols is not None:
        results: DeltaTable = table_dlt.executeZOrderBy(partition_cols)
    else:
        results: DeltaTable = table_dlt.executeCompaction()

    # Return
    return results if return_result else None


@typechecked
def optimise_table(
    table_name: str,
    table_path: str,
    spark_session: SparkSession,
    partition_cols: Optional[str_collection] = None,
    inspect: bool = False,
    return_result: bool = True,
    method: Literal["api", "sql"] = "api",
    conditional_where_clause: Optional[str] = None,
) -> Optional[psDataFrame]:
    """
    !!! note "Summary"
        Run the `OPTIMIZE` command over a [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) table to ensure that it is structurally efficient.

    ???+ abstract "Details"
        There are fundamentally two different ways in which this optimisation process can be achieved: by SQL or by API. Under the hood, both of these two methods will be implemented the same way, over the [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable) object, however the syntactic method to execute the optimisation allows for flexibility through either a Python API method or a SQL method.

    Params:
        table_name (str):
            The name of the table to be optimised. Must be a valid `delta` table, and must exist in the `write_path` location.
        table_path (str):
            The location for where the `delta` table is located.<br>
        spark_session (SparkSession):
            The SparkSession to use for loading the table.
        partition_cols (Optional[Union[str, List[str]]], optional):
            The columns to be partitioned/clustered by.

            - If type `#!py list`, then these elements will be added to the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command, like this: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` ZORDER BY (col1, col2)``.
            - If type `#!py str`, then will be coerced to list of 1 elements long, like: `[partition_cols]`, then appended to the command, like: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` ZORDER BY (col1)``.
            - If `#!py None`, then nothing will be added to the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command.

            Default: `#!py None`.
        inspect (bool, optional):
            For debugging.
            If `#!py True`, then the `OPTIMIZE` command will be printed to the terminal.<br>
            Default: `#!py False`.
        return_result (bool, optional):
            For efficient handling of elements.
            If `#!py True`, then the table created by the `OPTIMIZE` command will be returned from the function.
            Noting that this table will give the statistics of what/how the `delta` table is optimised.<br>
            Default: `#!py True`.
        method (Literal["api", "sql"], optional):
            The method to use for the execution, either by `api` or `sql`.<br>
            Using `api` is preferred.<br>
            Default: `#!py "api"`.
        conditional_where_clause (Optional[str], optional):
            An optional conditional parameter to add to the command.<br>
            Any records matching this condition will be optimised; those not matching will not be optimised.<br>
            This is particularly useful for partitioned tables when you don't want to use ZORDER optimisation, or when you have huge tables.<br>
            Default: `#!py None`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (Union[psDataFrame, None]):
            Either `#!py None` or the statistics/details from the optimised delta table.

    ???+ info "Notes"
        ???+ info "Important notes"
            - For `partition_cols`:
                - If it is type `#!py list`, then the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command will be extended to include each element in the `partition_cols` `#!py list`. Like this: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` ZORDER BY (col1, col2)``.
                - If `partition_cols` is a type `#!py str`, then it will be coerced to a list of 1 elements, and then appended like mentioned above.
                - If `partition_cols` is `#!py None`, then nothing will be added to the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command.
            - For `conditional_where_clause`:
                - It must be a `#!py str`.
                - It must be in the format: `#!sql {column} {conditional} {value}`.
                - For example: `#!sql editdatetime >= '2023-09-01'`
                - This will then be coerced in to the format: `#!sql WHERE {where}`.
                - And then appended to the overall SQL command like this: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` WHERE {where}``.
        ???+ info "The `sql` process"
            When `#!py method=="sql"` then this process will:

            1. Take the table given by the param `table_name`.
            1. Build the SQL command using the values in the parameters `partition_cols` and `conditional_where_clause`.
            1. Will execute the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` WHERE {where} ZORDER BY {zorder}`` command over the new table.
            1. Optionally return the results.
        ???+ info "The `api` process"
            When `#!py method=="api"` then this process will:

            1. Take the table given by the param `table_name`.
            1. Build the partition columns when the `partition_cols` is not `#!py None`.
            1. Load the [`DeltaOptimizeBuilder`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaOptimizeBuilder) by using the syntax: `#!py table = DeltaTable.forPath(spark_session, f"{table_path}/{table_name}").optimize()`.
            1. Optionally add a where clause using the [`.where`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaOptimizeBuilder.where) when `conditional_where_clause` is not `#!py None`.
            1. Conditionally execute [`.executeZOrderBy`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaOptimizeBuilder.executeZOrderBy) when `partition_cols` is not `#!py None`, or [`.executeCompaction`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaOptimizeBuilder.executeCompaction) otherwise.

    ??? question "References"
        For more information, please see:

        - https://docs.azuredatabricks.net/_static/notebooks/delta/optimize-python.html
        - https://medium.com/@debusinha2009/cheatsheet-on-understanding-zorder-and-optimize-for-your-delta-tables-1556282221d3
        - https://www.cloudiqtech.com/partition-optimize-and-zorder-delta-tables-in-azure-databricks/
        - https://docs.databricks.com/delta/optimizations/file-mgmt.html
        - https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html
        - https://stackoverflow.com/questions/65320949/parquet-vs-delta-format-in-azure-data-lake-gen-2-store?_sm_au_=iVV4WjsV0q7WQktrJfsTkK7RqJB10
        - https://www.i-programmer.info/news/197-data-mining/12582-databricks-delta-adds-faster-parquet-import.html#:~:text=Databricks%20says%20Delta%20is%2010,data%20management%2C%20and%20query%20serving.

    ??? tip "See also"
        - [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
        - [`SparkSession.sql()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
        - [pyspark.sql.DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.forPath()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.forPath)
        - [`DeltaTable.optimize()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.optimize)
        - [`DeltaTable.executeZOrderBy()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.executeZOrderBy)
        - [`DeltaTable.executeCompaction()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.executeCompaction)
    """
    if method == "api":
        return _optimise_table_api(
            table_name=table_name,
            table_path=table_path,
            spark_session=spark_session,
            partition_cols=partition_cols,
            inspect=inspect,
            return_result=return_result,
            conditional_where_clause=conditional_where_clause,
        )
    elif method == "sql":
        return _optimise_table_sql(
            table_name=table_name,
            table_path=table_path,
            spark_session=spark_session,
            partition_cols=partition_cols,
            inspect=inspect,
            return_result=return_result,
            conditional_where_clause=conditional_where_clause,
        )


@typechecked
def retry_optimise_table(
    table_name: str,
    table_path: str,
    spark_session: SparkSession,
    partition_cols: Optional[str_collection] = None,
    inspect: bool = False,
    return_result: bool = True,
    method: Literal["api", "sql"] = "api",
    conditional_where_clause: Optional[str] = None,
    retry_exceptions: Union[
        type[Exception],
        list[Type[Exception]],
        tuple[Type[Exception], ...],
    ] = Exception,
    retry_attempts: int = 10,
) -> Optional[psDataFrame]:
    """
    !!! note "Summary"
        Retry the execution of [`optimise_table`][toolbox_pyspark.delta.optimise_table] a number of times when a given error exception is met.

    ???+ abstract "Details"

        Particularly useful for when you are trying to run this optimisation over a cluster, and when parallelisaiton is causing multiple processes to occur over the same DeltaTable at the same time.

        For more info on the Retry process, see: [`stamina.retry()`](https://stamina.hynek.me/en/stable/).

    Params:
        table_name (str):
            The name of the table to be optimised. Must be a valid `delta` table, and must exist in the `write_path` location.

        table_path (str):
            The location for where the `delta` table is located.

        spark_session (SparkSession):
            The SparkSession to use for loading the table.

        partition_cols (Optional[Union[str, List[str]]], optional):
            The columns to be partitioned/clustered by.

            - If type `#!py list`, then these elements will be added to the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command, like this: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` ZORDER BY (col1, col2)``.
            - If type `#!py str`, then will be coerced to list of 1 elements long, like: `[partition_cols]`, then appended to the command, like: ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` ZORDER BY (col1)``.
            - If `#!py None`, then nothing will be added to the ``#!sql OPTIMIZE delta.`{table_path}/{table_name}` `` command.

            Default: `#!py None`.

        inspect (bool, optional):
            For debugging.
            If `#!py True`, then the `OPTIMIZE` command will be printed to the terminal.<br>
            Default: `#!py False`.

        return_result (bool, optional):
            For efficient handling of elements.
            If `#!py True`, then the table created by the `OPTIMIZE` command will be returned from the function.
            Noting that this table will give the statistics of what/how the `delta` table is optimised.<br>
            Default: `#!py True`.

        method (Literal["api", "sql"], optional):
            The method to use for the execution, either by `api` or `sql`.<br>
            Using `api` is preferred.<br>
            Default: `#!py "api"`.

        conditional_where_clause (Optional[str], optional):
            An optional conditional parameter to add to the command.<br>
            Any records matching this condition will be optimised; those not matching will not be optimised.<br>
            This is particularly useful for partitioned tables when you don't want to use ZORDER optimisation, or when you have huge tables.<br>
            Default: `#!py None`.

        retry_exceptions (Union[ Type[Exception], List[Type[Exception]], Tuple[Type[Exception], ...], ], optional):
            A given single or collection of expected exceptions for which to catch and retry for.<br>
            Defaults to `#!py Exception`.

        retry_attempts (int, optional):
            The number of retries to attempt. If the underlying process is still failing after this number of attempts, then throw a hard error and alert the user.<br>
            Defaults to `#!py 10`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.

    Returns:
        (Union[psDataFrame, None]):
            Either `#!py None` or the statistics/details from the optimised delta table.

    ??? tip "See also"
        - [`stamina.retry()`](https://stamina.hynek.me/en/stable/)
        - [`optimise_table()`][toolbox_pyspark.delta.optimise_table]
        - [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
        - [`SparkSession.sql()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
        - [pyspark.sql.DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.forPath()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.forPath)
        - [`DeltaTable.optimize()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.optimize)
        - [`DeltaTable.executeZOrderBy()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.executeZOrderBy)
        - [`DeltaTable.executeCompaction()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.executeCompaction)
    """

    @retry(
        on=((*retry_exceptions,) if isinstance(retry_exceptions, list) else retry_exceptions),
        attempts=retry_attempts,
    )
    @typechecked
    def _retry_optimise_table(
        table_name: str,
        table_path: str,
        spark_session: SparkSession,
        partition_cols: Optional[str_collection] = None,
        inspect: bool = False,
        return_result: bool = True,
        method: Literal["api", "sql"] = "api",
        conditional_where_clause: Optional[str] = None,
    ) -> Optional[psDataFrame]:
        return optimise_table(
            table_name=table_name,
            table_path=table_path,
            spark_session=spark_session,
            partition_cols=partition_cols,
            inspect=inspect,
            return_result=return_result,
            method=method,
            conditional_where_clause=conditional_where_clause,
        )

    return _retry_optimise_table(
        table_name=table_name,
        table_path=table_path,
        spark_session=spark_session,
        partition_cols=partition_cols,
        inspect=inspect,
        return_result=return_result,
        method=method,
        conditional_where_clause=conditional_where_clause,
    )


# ---------------------------------------------------------------------------- #
#  Merging processes                                                        ####
# ---------------------------------------------------------------------------- #


@typechecked
def merge_spark_to_delta(
    from_table: psDataFrame,
    to_table_name: str,
    to_table_path: str,
    matching_keys: Optional[str_collection] = None,
    from_keys: Optional[str_collection] = None,
    to_keys: Optional[str_collection] = None,
    partition_keys: Optional[str_dict] = None,
    editdate_col_name: Optional[str] = "editdate",
    delete_unmatched_rows: Optional[bool] = False,
    enable_automatic_schema_evolution: Optional[bool] = False,
    return_merge_metrics: Optional[bool] = False,
) -> Union[bool, psDataFrame]:
    """
    !!! note "Summary"
        Take one PySpark DataFrame `from_table`, and merge it with another DeltaTable at location: `to_table_path`/`to_table_name`.

    Params:
        from_table (psDataFrame):
            The PySpark table. Data will be merged FROM here.
        to_table_name (str):
            The name of the Delta table. Data will be merged TO here.
        to_table_path (str):
            The location where the target Delta table can be found.
        matching_keys (Optional[Union[str, List[str], Tuple[str, ...], Set[str]]], optional):
            The list of matching columns between both the Spark table and the Delta table.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[matching_keys]`.<br>
            If this is not provided, then BOTH the `from_keys` and the `to_keys` must be provided.<br>
            Defaults to `#!py None`.
        from_keys (Optional[Union[str, List[str], Tuple[str, ...], Set[str]]], optional):
            The list of keys on the `from_table` to use in the join.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[from_keys]`.<br>
            Only necessary when `matching_keys` is `#!py None`. When provided, the length must be the same as the `to_keys`.<br>
            Defaults to `#!py None`.
        to_keys (Optional[Union[str, List[str], Tuple[str, ...], Set[str]]], optional):
            The list of keys on the `to_table` to use in the join.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[to_keys]`.<br>
            Only necessary when `matching_keys` is `#!py None`. When provided, the length must be the same as the `from_keys`.<br>
            Defaults to `#!py None`.
        partition_keys (Optional[Dict[str, str]], optional):
            The keys and values that the `to_table` is partitioned by.<br>
            This is to improve (Concurrency Control)[https://docs.delta.io/latest/concurrency-control.html] while performing the merges.<br>
            If provided, it will enhance the internal `join_keys` variable to add new clauses for each column and value provided, to ensure it is explicit and direct.<br>
            If provided, it must be a `#!py dict`, where the keys are the columns and the values are the specific partition to use.<br>
            For example, if `partition_keys` is `{'SYSTEM':'OWAU','WHSEID':'BNE04'}`, then the `join_keys` will be enhanced to add `... and TRG.SYSTEM='OWAU' and TRG.WHSEID='BNE04'. Which will then execute where the partition for `SYSTEM` will _only_ implement for the `OWAU` value, and same for `WHSEID`.
            Defaults to `#!py None`.
        editdate_col_name (Optional[str], optional):
            The column to use for the `editdate` field, in case any table uses a different name for this field.<br>
            If not provided (as in, the value `#!py None` is parsed to this parameter), then this function will not implement any conditional logic during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.<br>
            Defaults to `#!py "editdate"`.
        delete_unmatched_rows (Optional[bool], optional):
            Whether or not to **DELETE** rows on the _target_ table which are existing on the _target_ but missing from the _source_ tables.<br>
            This should be used if you want to clean the target table and delete any rows which have already been deleted from the source table.<br>
            If `#!py True`, then this function will implement the method [`.whenNoMatchedBySourceDelete()`](https://docs.delta.io/latest/api/python/spark/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedBySourceDelete) method, with no conditionals.<br>
            Defaults to `#!py False`.
        enable_automatic_schema_evolution (Optional[bool], optional):
            Optional parameter for whether or not to automatically update the downstream `delta` table schema.<br>
            As documented extensively elsewhere:

            - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge
            - https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html
            - https://www.databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html
            - https://towardsdatascience.com/delta-lake-automatic-schema-evolution-11d32bd1aa99

            Defaults to `#!py False`.
        return_merge_metrics (Optional[bool], optional):
            Set to `#!py True` if you want to return the Merge metrics from this function.<br>
            If `#!py False`, it will only return the value: `#!py True`.<br>
            Defaults to `#!py False`.

    Returns:
        (Union[bool, psDataFrame]):
            Will return either:

            - If `return_merge_metrics` is `#!py True`: Will return the Merge metrics, which is calculated by:
                1. Extracting the history from DeltaTable (at the `to_table_path` location),
                1. Coercing that history object to a `pyspark` DataFrame,
                1. Filtering to only extract the `#!sql MERGE` operations,
                1. Limiting to the top `#!py 1` lines, which is the most recent info.
            - If `return_merge_metrics` is `#!py False`: The value `#!py True` is returned when the function runs successfully.

            If an error is thrown, then obviously it will not reach this far.
            Unfortunately, the DeltaTable Merge process does not return any data or statistics from it's execution... So therefore, we need to use the DeltaTable history to fetch the metrics. For more info, see: [Show key metrics after running `.merge(...)....execute()`](https://github.com/delta-io/delta/issues/1361)

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            - If any of `matching_keys` do not exist in the Spark table
            - If any of `matching_keys` do not exist in the Delta table
            - If any of `from_keys` do not exist in the Spark table
            - If any of `to_keys` do not exist in the Delta table
        AssertionError:
            - If `matching_keys` is None AND `from_keys` is None
            - If `matching_keys` is None AND `to_keys` is None
            - If length of `from_keys` does not match the length of `to_keys`

    ???+ info "Notes"
        The main objective of this function is to:

        1. For any records _existing_ in Spark but _missing_ in Delta, then INSERT those records from Spark to Delta. Using the [`.whenNotMatchedInsertAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedInsertAll) method.
        1. For any records _existing_ in both Spark and Delta, check if they have been _updated_ in Spark and if so then UPDATE those matching records in the Delta. Using the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.
        1. Conditionally, check whether or not to actually apply #2 above by comparing the `editdate_col_name` field between the two tables.

        Note:

        1. The `from_keys` and the `to_keys` will logically be the same values MOST of the time.
            - Very rarely will they ever be different; however, they are added here as separate parameters to facilitate this future functionality.
        1. If `from_keys` and `to_keys` are type `#!py list`, then their length must be the same.
        1. Conditional logic is applied during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method to avoid re-updating data in the Delta location which has actually updated from the SpSpark table.
        1. There is an additional `#!sql ifnull()` conditional check added to the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method for converting any values in the _target_ table to `#!py timestamp(0)` when their value is actually `#!sql null`.
            - The history to this check is that when these data were originally added to BigDaS, the column `EditDate` did not exist.
            - Therefore, when they were first inserted, all the values in `EditDate` were `#!sql null`.
            - As time progressed, the records have slowly been updating, and therefore the `EditDate` values have been changing.
            - Due to nuances and semantics around how Spark handles `null` values, whenever this previous check was run including columns with values `#!sql null`, it would inevitably return `#!sql null`.
            - As such, these rows were not identified as able to be matched, therefore the optimiser skipped them.
            - However, we actually did want them to be matched; because the rows had actually been updated on the _source_ table.
            - Therefore, we add this `#!sql ifnull()` check to capture this edge case, and then push through and update the record on the _target_ table.
        1. The parameter `enable_automatic_schema_evolution` was added because it is possible for the upstream tables to be adding new columns as they evolve. Therefore, it is necessary for this function to handle schema evolution automatically.

    ???+ question "References"
        - https://docs.databricks.com/delta/delta-update.html#language-python
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge&language-python
        - https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder
        - https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

    ???+ tip "See also"
        - [`load_table()`][toolbox_pyspark.delta.load_table]
        - [`assert_columns_exists()`][toolbox_pyspark.delta.assert_columns_exists]
        - [`get_columns()`][toolbox_pyspark.delta.get_columns]
        - [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
        - [`SparkSession.sql()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
        - [`DeltaMergeBuilder`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder)
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.history()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.history)
    """

    # Set up
    SRC_ALIAS = "src"
    TRG_ALIAS = "dlt"
    spark_session: SparkSession = from_table.sparkSession

    # Enable automatic Schema Evolution
    if enable_automatic_schema_evolution:
        current_conf = spark_session.conf.get(
            "spark.databricks.delta.schema.autoMerge.enabled",
        )
        _ = spark_session.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    # Define target table
    to_table = load_table(
        name=to_table_name,
        path=to_table_path,
        spark_session=spark_session,
    )

    # Check keys
    if matching_keys is not None:
        matching_keys = get_columns(from_table, matching_keys)
        assert_columns_exists(from_table, matching_keys, match_case=False)
        assert_columns_exists(to_table.toDF(), matching_keys, match_case=False)
        join_keys: str = " and ".join(
            [f"{SRC_ALIAS}.{key}={TRG_ALIAS}.{key}" for key in matching_keys]
        )
    else:
        assert from_keys is not None, f"Cannot be `None`: '{from_keys=}'"
        assert to_keys is not None, f"Cannot be `None`: '{to_keys=}'"
        from_keys = get_columns(from_table, from_keys)
        to_keys = get_columns(to_table.toDF(), to_keys)
        if not len(from_keys) == len(to_keys):
            raise ValueError(f"`from_keys` & `to_keys` must be the same length.")
        assert_columns_exists(from_table, from_keys, match_case=False)
        assert_columns_exists(to_table.toDF(), to_keys, match_case=False)
        combined_keys = zip(from_keys, to_keys)
        for from_key, to_key in combined_keys:
            assert from_key == to_key, f"Must be same: '{from_key=}' & '{to_key=}'"
        join_keys = " and ".join(
            [
                f"{SRC_ALIAS}.{from_key}={TRG_ALIAS}.{to_key}"
                for from_key, to_key in combined_keys
            ]
        )
    if partition_keys is not None:
        assert_columns_exists(to_table.toDF(), list(partition_keys.keys()))
        # TODO: Add a check for the `values`??
        for key, value in partition_keys.items():
            join_keys += f" and {TRG_ALIAS}.{key}='{value}'"

    # Run
    merger: DeltaMergeBuilder = (
        to_table.alias(TRG_ALIAS)
        .merge(
            source=from_table.alias(SRC_ALIAS),
            condition=join_keys,
        )
        .whenMatchedUpdateAll(
            condition=(
                None
                if editdate_col_name is None
                else f"ifnull({TRG_ALIAS}.{editdate_col_name}, timestamp(0))<{SRC_ALIAS}.{editdate_col_name}"
            )
        )
        .whenNotMatchedInsertAll()
    )
    if delete_unmatched_rows:
        merger = merger.whenNotMatchedBySourceDelete()
    merger.execute()

    # Return settings
    if enable_automatic_schema_evolution and current_conf is not None:
        _ = spark_session.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled",
            current_conf,
        )

    # Return
    if return_merge_metrics:
        return (
            to_table.history()
            .filter("operation='MERGE'")
            .limit(1)
            .select(
                "version",
                "timestamp",
                "operation",
                "operationParameters",
                "operationMetrics",
            )
        )
    else:
        return True


@typechecked
def merge_delta_to_delta(
    from_table_name: str,
    from_table_path: str,
    to_table_name: str,
    to_table_path: str,
    spark_session: SparkSession,
    matching_keys: str_collection,
    partition_keys: Optional[str_dict] = None,
    editdate_col_name: Optional[str] = "editdate",
    delete_unmatched_rows: Optional[bool] = False,
    enable_automatic_schema_evolution: Optional[bool] = False,
    return_merge_metrics: Optional[bool] = False,
) -> Union[bool, psDataFrame]:
    """
    !!! note "Summary"
        Take one DeltaTable at location`from_table_path`/`from_table_name`, and merge it with another DeltaTable at location: `to_table_path`/`to_table_name`.

    ???+ abstract "Details"
        This function is fundamentally the same as the [`merge_spark_to_delta()`][toolbox_pyspark.delta.merge_spark_to_delta] function, except it defines the `from_table` as a DeltaTable instead of a Spark DataFrame.

    Params:
        from_table_name (str):
            The name of the Delta table. Data will be merged FROM here.
        from_table_path (str):
            The location where the source Delta table can be found.
        to_table_name (str):
            The name of the Delta table. Data will be merged TO here.
        to_table_path (str):
            The location where the target Delta table can be found.
        spark_session (SparkSession):
            The Spark session to use for the merging.
        matching_keys (Union[str, List[str], Tuple[str, ...]]):
            The list of matching columns between both the Spark table and the Delta table.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[matching_keys]`.<br>
            If this is not provided, then BOTH the `from_keys` and the `to_keys` must be provided.<br>
            Defaults to `#!py None`.
        editdate_col_name (Optional[str], optional):
            The column to use for the `editdate` field, in case any table uses a different name for this field.<br>
            If not provided (as in, the value `#!py None` is parsed to this parameter), then this function will not implement any conditional logic during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.<br>
            Defaults to `#!py "editdate"`.
        delete_unmatched_rows (Optional[bool], optional):
            Whether or not to **DELETE** rows on the _target_ table which are existing on the _target_ but missing from the _source_ tables.<br>
            This should be used if you want to clean the target table and delete any rows which have already been deleted from the source table.<br>
            If `#!py True`, then this function will implement the method [`.whenNoMatchedBySourceDelete()`](https://docs.delta.io/latest/api/python/spark/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedBySourceDelete) method, with no conditionals.<br>
            Defaults to `#!py False`.
        enable_automatic_schema_evolution (Optional[bool], optional):
            Optional parameter for whether or not to automatically update the downstream `delta` table schema.<br>
            As documented extensively elsewhere:

            - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge
            - https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html
            - https://www.databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html
            - https://towardsdatascience.com/delta-lake-automatic-schema-evolution-11d32bd1aa99

            Defaults to `#!py False`.
        return_merge_metrics (Optional[bool], optional):
            Set to `#!py True` if you want to return the Merge metrics from this function.<br>
            If `#!py False`, it will only return the value: `#!py True`.<br>
            Defaults to `#!py False`.

    Returns:
        (Union[bool, psDataFrame]):
            Will return either:

            - If `return_merge_metrics` is `#!py True`: Will return the Merge metrics, which is calculated by:
                1. Extracting the history from DeltaTable (at the `to_table_path` location),
                1. Coercing that history object to a `pyspark` DataFrame,
                1. Filtering to only extract the `#!sql MERGE` operations,
                1. Limiting to the top `#!py 1` lines, which is the most recent info.
            - If `return_merge_metrics` is `#!py False`: The value `#!py True` is returned when the function runs successfully.

            If an error is thrown, then obviously it will not reach this far.
            Unfortunately, the DeltaTable Merge process does not return any data or statistics from it's execution... So therefore, we need to use the DeltaTable history to fetch the metrics. For more info, see: [Show key metrics after running `.merge(...)....execute()`](https://github.com/delta-io/delta/issues/1361)

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            - If any of `matching_keys` do not exist in the Spark table
            - If any of `matching_keys` do not exist in the Delta table
            - If any of `from_keys` do not exist in the Spark table
            - If any of `to_keys` do not exist in the Delta table
        AssertionError:
            - If `matching_keys` is None AND `from_keys` is None
            - If `matching_keys` is None AND `to_keys` is None
            - If length of `from_keys` does not match the length of `to_keys`

    ??? info "Notes"
        The main objective of this function is to:

        1. For any records _existing_ in Spark but _missing_ in Delta, then INSERT those records from Spark to Delta. Using the [`.whenNotMatchedInsertAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedInsertAll) method.
        1. For any records _existing_ in both Spark and Delta, check if they have been _updated_ in Spark and if so then UPDATE those matching records in the Delta. Using the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.
        1. Conditionally, check whether or not to actually apply #2 above by comparing the `editdate_col_name` field between the two tables.

        Note:

        1. The `from_keys` and the `to_keys` will logically be the same values MOST of the time.
            - Very rarely will they ever be different; however, they are added here as separate parameters to facilitate this future functionality.
        1. If `from_keys` and `to_keys` are type `#!py list`, then their length must be the same.
        1. Conditional logic is applied during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method to avoid re-updating data in the Delta location which has actually updated from the SpSpark table.
        1. There is an additional `#!sql ifnull()` conditional check added to the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method for converting any values in the _target_ table to `#!py timestamp(0)` when their value is actually `#!sql null`.
            - The history to this check is that when these data were originally added to BigDaS, the column `EditDate` did not exist.
            - Therefore, when they were first inserted, all the values in `EditDate` were `#!sql null`.
            - As time progressed, the records have slowly been updating, and therefore the `EditDate` values have been changing.
            - Due to nuances and semantics around how Spark handles `null` values, whenever this previous check was run including columns with values `#!sql null`, it would inevitably return `#!sql null`.
            - As such, these rows were not identified as able to be matched, therefore the optimiser skipped them.
            - However, we actually did want them to be matched; because the rows had actually been updated on the _source_ table.
            - Therefore, we add this `#!sql ifnull()` check to capture this edge case, and then push through and update the record on the _target_ table.
        1. The parameter `enable_automatic_schema_evolution` was added because it is possible for the upstream tables to be adding new columns as they evolve. Therefore, it is necessary for this function to handle schema evolution automatically.

    ??? question "References"
        - https://docs.databricks.com/delta/delta-update.html#language-python
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge&language-python
        - https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder
        - https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

    ??? tip "See Also"
        - [`merge_spark_to_delta()`][toolbox_pyspark.delta.merge_spark_to_delta]
        - [`load_table()`][toolbox_pyspark.delta.load_table]
        - [`assert_columns_exists()`][toolbox_pyspark.delta.assert_columns_exists]
        - [`get_columns()`][toolbox_pyspark.delta.get_columns]
        - [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
        - [`SparkSession.sql()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
        - [`DeltaMergeBuilder`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder)
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.history()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.history)
    """
    from_table: DeltaTable = load_table(
        name=from_table_name,
        path=from_table_path,
        spark_session=spark_session,
    )
    return merge_spark_to_delta(
        from_table=from_table.toDF(),
        to_table_name=to_table_name,
        to_table_path=to_table_path,
        matching_keys=matching_keys,
        partition_keys=partition_keys,
        editdate_col_name=editdate_col_name,
        delete_unmatched_rows=delete_unmatched_rows,
        enable_automatic_schema_evolution=enable_automatic_schema_evolution,
        return_merge_metrics=return_merge_metrics,
    )


def retry_merge_spark_to_delta(
    from_table: psDataFrame,
    to_table_name: str,
    to_table_path: str,
    matching_keys: Optional[str_collection] = None,
    from_keys: Optional[str_collection] = None,
    to_keys: Optional[str_collection] = None,
    partition_keys: Optional[str_dict] = None,
    editdate_col_name: Optional[str] = "editdate",
    delete_unmatched_rows: Optional[bool] = False,
    enable_automatic_schema_evolution: Optional[bool] = False,
    return_merge_metrics: Optional[bool] = False,
    retry_exceptions: Union[
        type[Exception],
        list[Type[Exception]],
        tuple[Type[Exception], ...],
    ] = Exception,
    retry_attempts: int = 10,
) -> Union[bool, psDataFrame]:
    """
    !!! note "Summary"

        Take one PySpark DataFrame `from_table`, and merge it with another DeltaTable at location: `to_table_path`/`to_table_name`.

    ???+ abstract "Details"

        This function is fundamentally the same as the [`merge_spark_to_delta()`][toolbox_pyspark.delta.merge_spark_to_delta] function, except that it will automatically retry the merge function a number of times if it meets an error.

        Particularly useful for when you are trying to run this optimisation over a cluster, and when parallelisaiton is causing multiple processes to occur over the same DeltaTable at the same time.

        For more info on the Retry process, see: [`stamina.retry()`](https://stamina.hynek.me/en/stable/).

    Params:
        from_table (psDataFrame):
            The PySpark table. Data will be merged FROM here.

        to_table_name (str):
            The name of the Delta table. Data will be merged TO here.

        to_table_path (str):
            The location where the target Delta table can be found.

        matching_keys (Union[List[str], str], optional):
            The list of matching columns between both the Spark table and the Delta table.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[matching_keys]`.<br>
            If this is not provided, then BOTH the `from_keys` and the `to_keys` must be provided.<br>
            Defaults to `#!py None`.

        from_keys (Union[List[str], str], optional):
            The list of keys on the `from_table` to use in the join.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[from_keys]`.<br>
            Only necessary when `matching_keys` is `#!py None`. When provided, the length must be the same as the `to_keys`.<br>
            Defaults to `#!py None`.

        to_keys (Union[List[str], str], optional):
            The list of keys on the `to_table` to use in the join.<br>
            If this is parsed in as a `#!py str` type, then it will be coerced to a list like: `[to_keys]`.<br>
            Only necessary when `matching_keys` is `#!py None`. When provided, the length must be the same as the `from_keys`.<br>
            Defaults to `#!py None`.

        editdate_col_name (Optional[str], optional):
            The column to use for the `editdate` field, in case any table uses a different name for this field.<br>
            If not provided (as in, the value `#!py None` is parsed to this parameter), then this function will not implement any conditional logic during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.<br>
            Defaults to `#!py "editdate"`.

        delete_unmatched_rows (Optional[bool], optional):
            Whether or not to **DELETE** rows on the _target_ table which are existing on the _target_ but missing from the _source_ tables.<br>
            This should be used if you want to clean the target table and delete any rows which have already been deleted from the source table.<br>
            If `#!py True`, then this function will implement the method [`.whenNoMatchedBySourceDelete()`](https://docs.delta.io/latest/api/python/spark/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedBySourceDelete) method, with no conditionals.<br>
            Defaults to `#!py False`.

        enable_automatic_schema_evolution (Optional[bool], optional):
            Optional parameter for whether or not to automatically update the downstream `delta` table schema.<br>
            As documented extensively elsewhere:

            - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge
            - https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html
            - https://www.databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html
            - https://towardsdatascience.com/delta-lake-automatic-schema-evolution-11d32bd1aa99

            Defaults to `#!py False`.

        return_merge_metrics (Optional[bool], optional):
            Set to `#!py True` if you want to return the Merge metrics from this function.<br>
            If `#!py False`, it will only return the value: `#!py True`.<br>
            Defaults to `#!py False`.

        retry_exceptions (Union[ Type[Exception], List[Type[Exception]], Tuple[Type[Exception], ...], ], optional):
            A given single or collection of expected exceptions for which to catch and retry for.<br>
            Defaults to `#!py Exception`.

        retry_attempts (int, optional):
            The number of retries to attempt. If the underlying process is still failing after this number of attempts, then throw a hard error and alert the user.<br>
            Defaults to `#!py 10`.

    Returns:
        (Union[bool, psDataFrame]):
            Will return either:

            - If `return_merge_metrics` is `#!py True`: Will return the Merge metrics, which is calculated by:
                1. Extracting the history from DeltaTable (at the `to_table_path` location),
                1. Coercing that history object to a `pyspark` DataFrame,
                1. Filtering to only extract the `#!sql MERGE` operations,
                1. Limiting to the top `#!py 1` lines, which is the most recent info.
            - If `return_merge_metrics` is `#!py False`: The value `#!py True` is returned when the function runs successfully.

            If an error is thrown, then obviously it will not reach this far.
            Unfortunately, the DeltaTable Merge process does not return any data or statistics from it's execution... So therefore, we need to use the DeltaTable history to fetch the metrics. For more info, see: [Show key metrics after running `.merge(...)....execute()`](https://github.com/delta-io/delta/issues/1361)

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            - If any of `matching_keys` do not exist in the Spark table
            - If any of `matching_keys` do not exist in the Delta table
            - If any of `from_keys` do not exist in the Spark table
            - If any of `to_keys` do not exist in the Delta table
        AssertionError:
            - If `matching_keys` is None AND `from_keys` is None
            - If `matching_keys` is None AND `to_keys` is None
            - If length of `from_keys` does not match the length of `to_keys`

    ???+ info "Notes"

        ???+ info "The main objective of this function is to:"

            1. For any records _existing_ in Spark but _missing_ in Delta, then INSERT those records from Spark to Delta. Using the [`.whenNotMatchedInsertAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenNotMatchedInsertAll) method.
            1. For any records _existing_ in both Spark and Delta, check if they have been _updated_ in Spark and if so then UPDATE those matching records in the Delta. Using the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method.
            1. Conditionally, check whether or not to actually apply #2 above by comparing the `editdate_col_name` field between the two tables.

        ???+ info "Pay particular attention to:"

            1. The `from_keys` and the `to_keys` will logically be the same values MOST of the time.
                - Very rarely will they ever be different; however, they are added here as separate parameters to facilitate this future functionality.
            1. If `from_keys` and `to_keys` are type `#!py list`, then their length must be the same.
            1. Conditional logic is applied during the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method to avoid re-updating data in the Delta location which has actually updated from the SpSpark table.
            1. There is an additional `#!sql ifnull()` conditional check added to the [`.whenMatchedUpdateAll()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder.whenMatchedUpdateAll) method for converting any values in the _target_ table to `#!py timestamp(0)` when their value is actually `#!sql null`.
                - The history to this check is that when these data were originally added to BigDaS, the column `EditDate` did not exist.
                - Therefore, when they were first inserted, all the values in `EditDate` were `#!sql null`.
                - As time progressed, the records have slowly been updating, and therefore the `EditDate` values have been changing.
                - Due to nuances and semantics around how Spark handles `null` values, whenever this previous check was run including columns with values `#!sql null`, it would inevitably return `#!sql null`.
                - As such, these rows were not identified as able to be matched, therefore the optimiser skipped them.
                - However, we actually did want them to be matched; because the rows had actually been updated on the _source_ table.
                - Therefore, we add this `#!sql ifnull()` check to capture this edge case, and then push through and update the record on the _target_ table.
            1. The parameter `enable_automatic_schema_evolution` was added because it is possible for the upstream tables to be adding new columns as they evolve. Therefore, it is necessary for this function to handle schema evolution automatically.

    ???+ question "References"
        - https://docs.databricks.com/delta/delta-update.html#language-python
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge&language-python
        - https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder
        - https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html
        - https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

    ???+ tip "See also"
        - [`stamina.retry()`](https://stamina.hynek.me/en/stable/)
        - [`merge_spark_to_delta()`][toolbox_pyspark.delta.merge_spark_to_delta]
        - [`load_table()`][toolbox_pyspark.delta.load_table]
        - [`assert_columns_exists()`][toolbox_pyspark.delta.assert_columns_exists]
        - [`get_columns()`][toolbox_pyspark.delta.get_columns]
        - [`SparkSession`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
        - [`SparkSession.sql()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html)
        - [`DeltaMergeBuilder`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaMergeBuilder)
        - [`DeltaTable`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable)
        - [`DeltaTable.history()`](https://docs.delta.io/latest/api/python/index.html#delta.tables.DeltaTable.history)
    """

    @retry(
        on=((*retry_exceptions,) if isinstance(retry_exceptions, list) else retry_exceptions),
        attempts=retry_attempts,
    )
    @typechecked
    def _retry_merge_spark_to_delta(
        from_table: psDataFrame,
        to_table_name: str,
        to_table_path: str,
        matching_keys: Optional[str_collection] = None,
        from_keys: Optional[str_collection] = None,
        to_keys: Optional[str_collection] = None,
        partition_keys: Optional[str_dict] = None,
        editdate_col_name: Optional[str] = "editdate",
        delete_unmatched_rows: Optional[bool] = False,
        enable_automatic_schema_evolution: Optional[bool] = False,
        return_merge_metrics: Optional[bool] = False,
    ) -> Union[bool, psDataFrame]:
        return merge_spark_to_delta(
            from_table=from_table,
            to_table_name=to_table_name,
            to_table_path=to_table_path,
            matching_keys=matching_keys,
            from_keys=from_keys,
            to_keys=to_keys,
            partition_keys=partition_keys,
            editdate_col_name=editdate_col_name,
            delete_unmatched_rows=delete_unmatched_rows,
            enable_automatic_schema_evolution=enable_automatic_schema_evolution,
            return_merge_metrics=return_merge_metrics,
        )

    return _retry_merge_spark_to_delta(
        from_table=from_table,
        to_table_name=to_table_name,
        to_table_path=to_table_path,
        matching_keys=matching_keys,
        from_keys=from_keys,
        to_keys=to_keys,
        partition_keys=partition_keys,
        editdate_col_name=editdate_col_name,
        delete_unmatched_rows=delete_unmatched_rows,
        enable_automatic_schema_evolution=enable_automatic_schema_evolution,
        return_merge_metrics=return_merge_metrics,
    )


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Classes                                                               ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  DeltaLoader                                                              ####
# ---------------------------------------------------------------------------- #


class DeltaLoader:
    """
    !!! note "Summary"
        A class to load and inspect Delta Lake tables from a specified root directory.

    ???+ abstract "Details"
        The `DeltaLoader` class provides methods to load Delta Lake tables from a specified root directory and inspect the contents of these tables. It uses the `dbutils` library if available to list folders, otherwise it falls back to using the `os` library.

    Params:
        root (str):
            The root directory where the Delta Lake tables are stored.
        spark (SparkSession):
            The Spark session to use for loading the Delta Lake tables.
        dbutils (optional):
            The `dbutils` library to use for listing folders. If not provided, the `os` library will be used.<br>
            Defaults to `None`.

    Methods:
        load(folder_name: str) -> psDataFrame:
            Load a Delta Lake table from the specified folder.

        folders() -> str_list:
            List the folders in the root directory.

        inspect() -> psDataFrame:
            Inspect the Delta Lake tables in the root directory and return a DataFrame with information about each table.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> from pyspark.sql import SparkSession
        >>> from toolbox_pyspark.delta import DeltaLoader
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create DeltaLoader instance
        >>> delta_loader = DeltaLoader(root="/path/to/delta/tables", spark=spark)
        ```

        ```{.py .python linenums="1" title="Example 1: Load a table"}
        >>> df = delta_loader.load("folder_name")
        >>> df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | 2 | 3 |
        | 4 | 5 | 6 |
        +---+---+---+
        ```
        !!! success "Conclusion: Successfully loaded the table from the specified folder."
        </div>

        ```{.py .python linenums="1" title="Example 2: List folders"}
        >>> folders = delta_loader.folders
        >>> print(folders)
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        ['folder1', 'folder2', 'folder3']
        ```
        !!! success "Conclusion: Successfully listed the folders in the root directory."
        </div>

        ```{.py .python linenums="1" title="Example 3: Inspect tables"}
        >>> inspection_df = delta_loader.inspect()
        >>> inspection_df.show()
        ```
        <div class="result" markdown>
        ```{.txt .text title="Terminal"}
        +---------+-------------+---------------------+-------+
        | Folder  | TimeElement | TimeStamp           | Count |
        +---------+-------------+---------------------+-------+
        | folder1 | EDITDATE    | 2023-01-01 00:00:00 |   100 |
        | folder2 | ADDDATE     | 2023-01-02 00:00:00 |   200 |
        | folder3 | None        | None                |   300 |
        +---------+-------------+---------------------+-------+
        ```
        !!! success "Conclusion: Successfully inspected the Delta Lake tables."
        </div>
    """

    def __init__(self, root: str, spark: SparkSession, dbutils=None) -> None:
        self._root: str = root
        self._spark_session: SparkSession = spark
        self._dbutils = dbutils

    def load(self, folder_name: str) -> psDataFrame:
        """
        !!! note "Summary"
            Load a Delta Lake table from the specified folder.

        ???+ abstract "Details"
            This method loads a Delta Lake table from the specified folder within the root directory. It uses the `read_from_path` function to read the data in Delta format.

        Params:
            folder_name (str):
                The name of the folder from which to load the Delta Lake table.

        Returns:
            (psDataFrame):
                The loaded Delta Lake table as a PySpark DataFrame.

        ???+ example "Examples"

            ```{.py .python linenums="1" title="Set up"}
            >>> # Imports
            >>> from pyspark.sql import SparkSession
            >>> from toolbox_pyspark.delta import DeltaLoader
            >>>
            >>> # Instantiate Spark
            >>> spark = SparkSession.builder.getOrCreate()
            >>>
            >>> # Create DeltaLoader instance
            >>> delta_loader = DeltaLoader(root="/path/to/delta/tables", spark=spark)
            ```

            ```{.py .python linenums="1" title="Example 1: Load a table"}
            >>> df = delta_loader.load("folder_name")
            >>> df.show()
            ```
            <div class="result" markdown>
            ```{.txt .text title="Terminal"}
            +---+---+---+
            | a | b | c |
            +---+---+---+
            | 1 | 2 | 3 |
            | 4 | 5 | 6 |
            +---+---+---+
            ```
            !!! success "Conclusion: Successfully loaded the table from the specified folder."
            </div>
        """
        return read_from_path(
            folder_name,
            self._root,
            spark_session=self._spark_session,
            data_format="delta",
        )

    @property
    def folders(self) -> str_list:
        """
        !!! note "Summary"
            List the folders in the root directory.

        ???+ abstract "Details"
            This property lists the folders in the root directory specified during the instantiation of the `DeltaLoader` class. It uses the `dbutils` library if available to list folders, otherwise it falls back to using the `os` library.

        Returns:
            (str_list):
                A list of folder names in the root directory.

        ???+ example "Examples"

            ```{.py .python linenums="1" title="Set up"}
            >>> # Imports
            >>> from pyspark.sql import SparkSession
            >>> from toolbox_pyspark.delta import DeltaLoader
            >>>
            >>> # Instantiate Spark
            >>> spark = SparkSession.builder.getOrCreate()
            >>>
            >>> # Create DeltaLoader instance
            >>> delta_loader = DeltaLoader(root="/path/to/delta/tables", spark=spark)
            ```

            ```{.py .python linenums="1" title="Example 1: List folders"}
            >>> folders = delta_loader.folders
            >>> print(folders)
            ```
            <div class="result" markdown>
            ```{.txt .text title="Terminal"}
            ['folder1', 'folder2', 'folder3']
            ```
            !!! success "Conclusion: Successfully listed the folders in the root directory."
            </div>
        """
        if self._dbutils is not None:
            return [
                folder.name.replace("/", "")
                for folder in self._dbutils.fs.ls(self._root)  # type:ignore
            ]
        else:
            return os.listdir(self._root)

    def inspect(self) -> psDataFrame:
        """
        !!! note "Summary"
            Inspect the Delta Lake tables in the root directory and return a DataFrame with information about each table.

        ???+ abstract "Details"
            This method inspects the Delta Lake tables in the root directory specified during the instantiation of the `DeltaLoader` class. It loads each table, checks for specific columns (`EDITDATE` and `ADDDATE`), and collects information about each table, including the folder name, the time element, the latest timestamp, and the row count.

        Returns:
            (psDataFrame):
                A DataFrame with information about each Delta Lake table in the root directory.

        ???+ example "Examples"

            ```{.py .python linenums="1" title="Set up"}
            >>> # Imports
            >>> from pyspark.sql import SparkSession
            >>> from toolbox_pyspark.delta import DeltaLoader
            >>>
            >>> # Instantiate Spark
            >>> spark = SparkSession.builder.getOrCreate()
            >>>
            >>> # Create DeltaLoader instance
            >>> delta_loader = DeltaLoader(root="/path/to/delta/tables", spark=spark)
            ```

            ```{.py .python linenums="1" title="Example 1: Inspect tables"}
            >>> inspection_df = delta_loader.inspect()
            >>> inspection_df.show()
            ```
            <div class="result" markdown>
            ```{.txt .text title="Terminal"}
            +---------+-------------+---------------------+-------+
            | Folder  | TimeElement | TimeStamp           | Count |
            +---------+-------------+---------------------+-------+
            | folder1 | EDITDATE    | 2023-01-01 00:00:00 |   100 |
            | folder2 | ADDDATE     | 2023-01-02 00:00:00 |   200 |
            | folder3 | None        | None                |   300 |
            +---------+-------------+---------------------+-------+
            ```
            !!! success "Conclusion: Successfully inspected the Delta Lake tables."
            </div>
        """
        data = []
        for folder in self.folders:
            df: psDataFrame = self.load(folder)
            cols: str_list = [col.upper() for col in df.columns]
            if "EDITDATE" in cols:
                data.append(
                    (
                        folder,
                        "EDITDATE",
                        df.select(F.max("EDITDATE")).first()["max(EDITDATE)"],
                        df.count(),
                    )
                )
            elif "ADDDATE" in cols:
                data.append(
                    (
                        folder,
                        "ADDDATE",
                        df.select(F.max("ADDDATE")).first()["max(ADDDATE)"],
                        df.count(),
                    )
                )
            else:
                data.append((folder, None, None, df.count()))
        return self._spark_session.createDataFrame(
            pd.DataFrame(data, columns=["Folder", "TimeElement", "TimeStamp", "Count"])
        )
