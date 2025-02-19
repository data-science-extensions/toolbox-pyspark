# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Future Python Library Imports ----
from __future__ import annotations

# ## Python StdLib Imports ----
import os
import sys
from functools import cached_property
from pathlib import Path
from string import ascii_letters
from typing import Callable, Union

# ## Python Third Party Imports ----
import pandas as pd
from pandas import DataFrame as pdDataFrame
from pyspark.sql import (
    DataFrame as psDataFrame,
    SparkSession,
    functions as F,
    types as T,
)
from toolbox_python.collection_types import any_list_tuple, str_list

# ## Local First Party Imports ----
from toolbox_pyspark.types import cast_columns_to_type


## --------------------------------------------------------------------------- #
##  Exports                                                                 ####
## --------------------------------------------------------------------------- #


__all__: str_list = [
    "name_func_flat_list",
    "name_func_nested_list",
    "name_func_predefined_name",
    "PySparkSetup",
]


## --------------------------------------------------------------------------- #
##  Helper functions                                                        ####
## --------------------------------------------------------------------------- #


def name_func_flat_list(
    func: Callable,
    idx: int,
    params: any_list_tuple,
) -> str:
    return f"{func.__name__}_{int(idx)+1:02}_{'_'.join([str(param) for param in params[0]])}"


def name_func_nested_list(
    func: Callable,
    idx: int,
    params: Union[list[any_list_tuple], tuple[any_list_tuple]],
) -> str:
    return f"{func.__name__}_{int(idx)+1:02}_{params[0][0]}_{params[0][1]}"


def name_func_predefined_name(
    func: Callable,
    idx: int,
    params: any_list_tuple,
) -> str:
    return f"{func.__name__}_{int(idx)+1:02}_{params[0][0]}"


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Generic Classes                                                       ####
#                                                                              #
# ---------------------------------------------------------------------------- #


class PySparkSetup:

    num_rows: int = 4

    @classmethod
    def set_up(cls) -> None:
        cls.setup_environ().setup_spark()

    @classmethod
    def tear_down(cls) -> None:
        cls.spark.stop()

    @classmethod
    def setup_environ(cls) -> PySparkSetup:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["HADOOP_HOME"] = str(Path(".venv").joinpath("bin", "pyspark").absolute())
        return cls

    @classmethod
    def setup_spark(cls) -> PySparkSetup:
        cls.spark: SparkSession = (
            SparkSession.builder.master("local").appName("unit_tests").getOrCreate()
        )
        return cls

    @staticmethod
    @F.udf
    def add_column_from_list(row, lst) -> str:
        return lst[row - 1]

    @cached_property
    def deprecation_message_regex(self) -> str:
        return "The .+ was deprecated since .+ in favor of .+"

    @cached_property
    def pd_df(self) -> pdDataFrame:
        """
        ```txt
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        | 2 | b |
        | 3 | c |
        | 4 | d |
        +---+---+
        ```
        """
        return pdDataFrame(
            {
                "a": range(1, self.num_rows + 1),
                "b": list(ascii_letters[: self.num_rows]),
            }
        )

    @cached_property
    def ps_df(self) -> psDataFrame:
        """
        ```txt
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        | 2 | b |
        | 3 | c |
        | 4 | d |
        +---+---+
        ```
        """
        return self.spark.createDataFrame(self.pd_df)

    @cached_property
    def ps_df_extended(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+
        | a | b | c | d |
        +---+---+---+---+
        | 1 | a | c | d |
        | 2 | b | c | d |
        | 3 | c | c | d |
        | 4 | d | c | d |
        +---+---+---+---+
        ```
        """
        return self.ps_df.withColumn("c", F.lit("c")).withColumn("d", F.lit("d"))

    @cached_property
    def ps_df_timestamp(self) -> psDataFrame:
        """
        ```txt
        +---+---+---------------------+---------------------+
        | a | b |              c_date |              d_date |
        +---+---+---------------------+---------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 |
        +---+---+---------------------+---------------------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c_date": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-01-01", periods=self.num_rows, freq="h")
                        )
                    ),
                ),
                "d_date": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-02-01", periods=self.num_rows, freq="h")
                        )
                    ),
                ),
            }
        ).transform(cast_columns_to_type, ["c_date", "d_date"], "datetime")

    @cached_property
    def ps_df_types(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+-----+-----+------------+---------------------+
        | a | b | c | d |   e |   f |          g |                   h |
        +---+---+---+---+-----+-----+------------+---------------------+
        | 1 | a | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 2 | b | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 3 | c | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        | 4 | d | 1 | 2 | 1.1 | 1.2 | 2022-01-01 | 2022-02-01 01:00:00 |
        +---+---+---+---+-----+-----+------------+---------------------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": F.lit("1").cast("int"),
                "d": F.lit("2").cast("string"),
                "e": F.lit("1.1").cast("float"),
                "f": F.lit("1.2").cast("double"),
                "g": F.lit("2022-01-01").cast("date"),
                "h": F.lit("2022-02-01 01:00:00").cast("timestamp"),
            }
        )

    @cached_property
    def pd_type_check(self) -> pdDataFrame:
        """
        ```txt
        +---+----------+-----------+
        |   | col_name |  col_type |
        +---+----------+-----------+
        | 1 |        a |    bigint |
        | 2 |        b |    string |
        | 3 |        c |       int |
        | 4 |        d |    string |
        | 5 |        e |     float |
        | 6 |        f |    double |
        | 7 |        g |      date |
        | 8 |        h | timestamp |
        +---+----------+-----------+
        ```
        """
        return pdDataFrame(
            self.ps_df_types.dtypes,
            columns=["col_name", "col_type"],
        )

    @cached_property
    def ps_df_timezone(self) -> psDataFrame:
        """
        ```txt
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        | a | b |                   c |                   d |                   e |          target | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Australia/Perth |   Australia/Perth |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Australia/Perth |   Australia/Perth |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Australia/Perth |   Australia/Perth |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Australia/Perth |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-01-01", periods=self.num_rows, freq="D")
                        )
                    ),
                ),
                "d": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-02-01", periods=self.num_rows, freq="D")
                        )
                    ),
                ),
                "e": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-03-01", periods=self.num_rows, freq="D")
                        )
                    ),
                ),
                "target": F.lit("Asia/Singapore"),
                "TIMEZONE_LOCATION": F.lit("Australia/Perth"),
            }
        ).transform(cast_columns_to_type, ["c", "d", "e"], "datetime")

    @cached_property
    def ps_df_timezone_extended(self) -> psDataFrame:
        """
        ```txt
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        | a | b |                   c |          d_datetime |          e_datetime |          target | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 | Australia/Perth |   Australia/Perth |
        | 2 | b | 2022-01-02 00:00:00 | 2022-02-02 00:00:00 | 2022-03-02 00:00:00 | Australia/Perth |   Australia/Perth |
        | 3 | c | 2022-01-03 00:00:00 | 2022-02-03 00:00:00 | 2022-03-03 00:00:00 | Australia/Perth |   Australia/Perth |
        | 4 | d | 2022-01-04 00:00:00 | 2022-02-04 00:00:00 | 2022-03-04 00:00:00 | Australia/Perth |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+-----------------+-------------------+
        ```
        """
        return self.ps_df_timezone.withColumnsRenamed(
            {"d": "d_datetime", "e": "e_datetime"},
        ).withColumns(
            {
                "d_datetime": F.to_timestamp("d_datetime"),
                "e_datetime": F.to_timestamp("e_datetime"),
            },
        )

    @cached_property
    def ps_df_datetime(self) -> psDataFrame:
        """
        ```txt
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | a | b |          c_datetime |          d_datetime |          e_datetime | TIMEZONE_LOCATION |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        | 1 | a | 2022-01-01 00:00:00 | 2022-02-01 00:00:00 | 2022-03-01 00:00:00 |   Australia/Perth |
        | 2 | b | 2022-01-01 01:00:00 | 2022-02-01 01:00:00 | 2022-03-01 01:00:00 |   Australia/Perth |
        | 3 | c | 2022-01-01 02:00:00 | 2022-02-01 02:00:00 | 2022-03-01 02:00:00 |   Australia/Perth |
        | 4 | d | 2022-01-01 03:00:00 | 2022-02-01 03:00:00 | 2022-03-01 03:00:00 |   Australia/Perth |
        +---+---+---------------------+---------------------+---------------------+-------------------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c_datetime": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-01-01", periods=self.num_rows, freq="h")
                        ),
                    ),
                ),
                "d_datetime": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-02-01", periods=self.num_rows, freq="h")
                        ),
                    ),
                ),
                "e_datetime": self.add_column_from_list(
                    "a",
                    F.lit(
                        list(
                            pd.date_range(start="2022-03-01", periods=self.num_rows, freq="h")
                        ),
                    ),
                ),
                "TIMEZONE_LOCATION": F.lit("Australia/Perth"),
            }
        ).transform(
            cast_columns_to_type, ["c_datetime", "d_datetime", "e_datetime"], "datetime"
        )

    @cached_property
    def ps_df_duplication(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+---+
        | a | b | c | d | n |
        +---+---+---+---+---+
        | 1 | a | 1 | 2 | a |
        | 2 | b | 1 | 2 | b |
        | 3 | c | 1 | 2 | c |
        | 4 | d | 1 | 2 | d |
        +---+---+---+---+---+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": F.lit(1),
                "d": F.lit("2"),
                "n": F.col("b"),
            }
        )

    @cached_property
    def ps_df_duplicates(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | 1 | 1 |
        | 2 | b | 1 | 2 | 1 |
        | 3 | c | 2 | 2 | 2 |
        | 4 | d | 2 | 2 | 3 |
        +---+---+---+---+---+
        ```
        """

        return self.ps_df.withColumns(
            {
                "c": self.add_column_from_list("a", F.lit([1, 1, 2, 2])),
                "d": self.add_column_from_list("a", F.lit([1, 2, 2, 2])),
                "e": self.add_column_from_list("a", F.lit([1, 1, 2, 3])),
            }
        )

    @cached_property
    def ps_df_dimensions(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+---+
        | a | b | c | d | e |
        +---+---+---+---+---+
        | 1 | a | 1 | a | x |
        | 2 | b | 1 | b | x |
        | 3 | c | 2 | b | y |
        | 4 | d | 2 | b | z |
        +---+---+---+---+---+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": self.add_column_from_list("a", F.lit(["1", "1", "2", "2"])),
                "d": self.add_column_from_list("a", F.lit(["a", "b", "b", "b"])),
                "e": self.add_column_from_list("a", F.lit(["x", "x", "y", "z"])),
            }
        )

    @cached_property
    def ps_df_trimming(self) -> psDataFrame:
        """
        ```txt
        +---+---+------+------+---------+
        | a | b |    c |    d |       e |
        +---+---+------+------+---------+
        | 1 | a | 1    |    2 |    3    |
        | 2 | b | 1    |    2 |    3    |
        | 3 | c | 1    |    2 |    3    |
        | 4 | d | 1    |    2 |    3    |
        +---+---+------+------+---------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": self.add_column_from_list("a", F.lit(["1   ", "1   ", "1   ", "1   "])),
                "d": self.add_column_from_list("a", F.lit(["   2", "   2", "   2", "   2"])),
                "e": self.add_column_from_list(
                    "a", F.lit(["   3   ", "   3   ", "   3   ", "   3   "])
                ),
            }
        )

    @cached_property
    def ps_df_keys(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+---+---+
        | a | b | c | d | e | f |
        +---+---+---+---+---+---+
        | 1 | a | 1 | 2 | 3 | 4 |
        | 2 | b | 1 | 2 | 3 | 4 |
        | 3 | c | 1 | 2 | 3 | 4 |
        | 4 | d | 1 | 2 | 3 | 4 |
        +---+---+---+---+---+---+
        ```
        """
        return (
            self.ps_df.withColumn("c", F.lit("1"))
            .withColumn("d", F.lit("2"))
            .withColumn("e", F.lit("3"))
            .withColumn("f", F.lit("4"))
        )

    @cached_property
    def ps_df_with_keys(self) -> psDataFrame:
        """
        ```txt
        +---+---+-------+---+---+-------+-------+
        | a | b | key_a | c | d | key_c | key_e |
        +---+---+-------+---+---+-------+-------+
        | 1 | a |     0 | 1 | 2 |     1 |     3 |
        | 2 | b |     1 | 1 | 2 |     1 |     3 |
        | 3 | c |     2 | 1 | 2 |     1 |     3 |
        | 4 | d |     3 | 1 | 2 |     1 |     3 |
        +---+---+-------+---+---+-------+-------+
        ```
        """
        return (
            self.ps_df.withColumn("key_a", F.col("a"))
            .withColumn("c", F.lit("1"))
            .withColumn("d", F.lit("2"))
            .withColumn("key_c", F.col("c"))
            .withColumn("key_e", F.lit("3"))
        )

    @cached_property
    def ps_df_schema_left(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+---+---+---+
        | a | b | c | d | e | f |
        +---+---+---+---+---+---+
        | 1 | a | 1 | 2 | 3 | 4 |
        | 2 | b | 1 | 2 | 3 | 4 |
        | 3 | c | 1 | 2 | 3 | 4 |
        | 4 | d | 1 | 2 | 3 | 4 |
        +---+---+---+---+---+---+
        ```
        """
        return self.ps_df_keys

    @cached_property
    def ps_df_schema_right(self) -> psDataFrame:
        """
        ```txt
        +---+---+---+------+---+---+
        | a | b | c |    d | f | g |
        +---+---+---+------+---+---+
        | 1 | a | 1 | null | 4 | a |
        | 2 | b | 1 | null | 4 | a |
        | 3 | c | 1 | null | 4 | a |
        | 4 | d | 1 | null | 4 | a |
        +---+---+---+------+---+---+
        ```
        """
        return (
            self.ps_df_keys.withColumn("c", F.col("c").cast("int"))
            .withColumn("g", F.lit("a"))
            .withColumn("d", F.lit("null"))
            .drop("e")
        )

    @cached_property
    def ps_df_formatting(self) -> psDataFrame:
        """
        ```txt
        +---+---+-----+-----+---------+------------+
        | a | b |   c |   d |       e |          f |
        +---+---+-----+-----+---------+------------+
        | 1 | a | 1.0 | 1.1 |    1000 |    1111.11 |
        | 2 | b | 2.0 | 2.2 |   10000 |   22222.22 |
        | 3 | c | 3.0 | 3.3 |  100000 |  333333.33 |
        | 4 | d | 4.0 | 4.4 | 1000000 | 4444444.44 |
        +---+---+-----+-----+---------+------------+
        ```
        """
        return self.ps_df.withColumns(
            {
                "c": self.add_column_from_list("a", F.lit([1.0, 2.0, 3.0, 4.0])).cast("float"),
                "d": self.add_column_from_list("a", F.lit([1.1, 2.2, 3.3, 4.4])).cast("float"),
                "e": self.add_column_from_list(
                    "a", F.lit([1000, 10000, 100000, 1000000])
                ).cast("int"),
                "f": self.add_column_from_list(
                    "a", F.lit([1111.11, 22222.22, 333333.33, 4444444.44])
                ).cast("float"),
            }
        )

    @cached_property
    def ps_df_decimals(self) -> psDataFrame:
        """
        ```txt
        +----+------------------------+------------------------+
        | a  | b                      | c                      |
        +----+------------------------+------------------------+
        | 1  | 1.10000000000000000000 | 1.60000000000000000000 |
        | 2  | 1.01000000000000000000 | 1.06000000000000000000 |
        | 3  | 1.00100000000000000000 | 1.00600000000000000000 |
        | 4  | 1.00010000000000000000 | 1.00060000000000000000 |
        | 5  | 1.00001000000000000000 | 1.00006000000000000000 |
        | 6  | 1.00000100000000000000 | 1.00000600000000000000 |
        | 7  | 1.00000010000000000000 | 1.00000060000000000000 |
        | 8  | 1.00000001000000000000 | 1.00000006000000000000 |
        | 9  | 1.00000000100000000000 | 1.00000000600000000000 |
        | 10 | 1.00000000010000000000 | 1.00000000060000000000 |
        | 11 | 1.00000000001000000000 | 1.00000000006000000000 |
        | 12 | 1.00000000000100000000 | 1.00000000000600000000 |
        | 13 | 1.00000000000010000000 | 1.00000000000060000000 |
        | 14 | 1.00000000000001000000 | 1.00000000000006000000 |
        | 15 | 1.00000000000000100000 | 1.00000000000000600000 |
        | 16 | 1.00000000000000010000 | 1.00000000000000060000 |
        | 17 | 1.00000000000000001000 | 1.00000000000000006000 |
        | 18 | 1.00000000000000000100 | 1.00000000000000000600 |
        | 19 | 1.00000000000000000010 | 1.00000000000000000060 |
        | 20 | 1.00000000000000000001 | 1.00000000000000000006 |
        +----+------------------------+------------------------+
        ```
        """
        rows = 20
        return self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "a": range(1, rows + 1),
                    "b": [f"1.{'0'*val}1" for val in range(1, rows + 1)],
                    "c": [f"1.{'0'*val}6" for val in range(1, rows + 1)],
                }
            )
        ).withColumns(
            {
                "b": F.col("b").cast(T.DecimalType(rows + 1, rows)),
                "c": F.col("c").cast(T.DecimalType(rows + 1, rows)),
            }
        )
