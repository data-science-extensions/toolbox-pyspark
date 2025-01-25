# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Imports                                                                 ####
## --------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from unittest import TestCase

# ## Python Third Party Imports ----
import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.collection_types import str_list

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.cleaning import apply_function_to_columns
from toolbox_pyspark.formatting import (
    display_intermediary_columns,
    display_intermediary_schema,
    display_intermediary_table,
    format_numbers,
)


## --------------------------------------------------------------------------- #
##  Initialisation                                                          ####
## --------------------------------------------------------------------------- #


def setUpModule() -> None:
    PySparkSetup.set_up()


def tearDownModule() -> None:
    PySparkSetup.tear_down()


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Cases                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  `format_numbers()`                                                      ####
## --------------------------------------------------------------------------- #


class TestFormatNumbers(PySparkSetup, TestCase):

    def test_format_numbers_01(self) -> None:
        result: psDataFrame = self.ps_df_formatting.transform(format_numbers)
        expected: psDataFrame = self.ps_df_formatting.transform(
            apply_function_to_columns, ("a", "e"), "format_number", 0
        ).transform(apply_function_to_columns, ("c", "d", "f"), "format_number", 2)
        assert_df_equality(result, expected)


## --------------------------------------------------------------------------- #
##  `display_intermediary_table()`                                          ####
## --------------------------------------------------------------------------- #


class TestDisplayIntermediaryTable(PySparkSetup, TestCase):

    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys: pytest.CaptureFixture) -> None:
        self.capsys: pytest.CaptureFixture = capsys

    def test_display_intermediary_table_01(self) -> None:
        self.ps_df_formatting.transform(
            display_intermediary_table, reformat_numbers=True, num_rows=2
        ).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "+---+---+----+----+------+---------+",
            "|a  |b  |c   |d   |e     |f        |",
            "+---+---+----+----+------+---------+",
            "|1  |a  |1.00|1.10|1,000 |1,111.11 |",
            "|2  |b  |2.00|2.20|10,000|22,222.22|",
            "+---+---+----+----+------+---------+",
            "only showing top 2 rows",
            "",
            "+---+---+---+---+-------+---------+",
            "|  a|  b|  c|  d|      e|        f|",
            "+---+---+---+---+-------+---------+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|",
            "|  3|  c|3.0|3.3| 100000|333333.34|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "",
        ]
        assert result == expected

    def test_display_intermediary_table_02(self) -> None:
        self.ps_df_formatting.transform(
            display_intermediary_table, reformat_numbers=True
        ).withColumn("c", F.expr("c * 2")).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "+---+---+----+----+---------+------------+",
            "|a  |b  |c   |d   |e        |f           |",
            "+---+---+----+----+---------+------------+",
            "|1  |a  |1.00|1.10|1,000    |1,111.11    |",
            "|2  |b  |2.00|2.20|10,000   |22,222.22   |",
            "|3  |c  |3.00|3.30|100,000  |333,333.34  |",
            "|4  |d  |4.00|4.40|1,000,000|4,444,444.50|",
            "+---+---+----+----+---------+------------+",
            "",
            "+---+---+---+---+-------+---------+",
            "|  a|  b|  c|  d|      e|        f|",
            "+---+---+---+---+-------+---------+",
            "|  1|  a|2.0|1.1|   1000|  1111.11|",
            "|  2|  b|4.0|2.2|  10000| 22222.22|",
            "|  3|  c|6.0|3.3| 100000|333333.34|",
            "|  4|  d|8.0|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "",
        ]
        assert result == expected

    def test_display_intermediary_table_03(self) -> None:
        self.ps_df_formatting.transform(
            display_intermediary_table, reformat_numbers=False
        ).withColumn("g", F.expr("c * 2")).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "+---+---+---+---+-------+---------+",
            "|a  |b  |c  |d  |e      |f        |",
            "+---+---+---+---+-------+---------+",
            "|1  |a  |1.0|1.1|1000   |1111.11  |",
            "|2  |b  |2.0|2.2|10000  |22222.22 |",
            "|3  |c  |3.0|3.3|100000 |333333.34|",
            "|4  |d  |4.0|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "+---+---+---+---+-------+---------+---+",
            "|  a|  b|  c|  d|      e|        f|  g|",
            "+---+---+---+---+-------+---------+---+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|2.0|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|4.0|",
            "|  3|  c|3.0|3.3| 100000|333333.34|6.0|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|8.0|",
            "+---+---+---+---+-------+---------+---+",
            "",
            "",
        ]
        assert result == expected


## --------------------------------------------------------------------------- #
##  `display_intermediary_schema()`                                         ####
## --------------------------------------------------------------------------- #


class TestDisplayIntermediarySchema(PySparkSetup, TestCase):

    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys: pytest.CaptureFixture) -> None:
        self.capsys: pytest.CaptureFixture = capsys

    def test_display_intermediary_schema_01(self) -> None:
        self.ps_df_formatting.transform(display_intermediary_schema).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "root",
            " |-- a: long (nullable = true)",
            " |-- b: string (nullable = true)",
            " |-- c: float (nullable = true)",
            " |-- d: float (nullable = true)",
            " |-- e: integer (nullable = true)",
            " |-- f: float (nullable = true)",
            "",
            "+---+---+---+---+-------+---------+",
            "|  a|  b|  c|  d|      e|        f|",
            "+---+---+---+---+-------+---------+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|",
            "|  3|  c|3.0|3.3| 100000|333333.34|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "",
        ]
        assert result == expected

    def test_display_intermediary_schema_02(self) -> None:
        self.ps_df_formatting.transform(display_intermediary_schema).withColumn(
            "c", F.expr("c * 2").cast("int").cast("string")
        ).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "root",
            " |-- a: long (nullable = true)",
            " |-- b: string (nullable = true)",
            " |-- c: float (nullable = true)",
            " |-- d: float (nullable = true)",
            " |-- e: integer (nullable = true)",
            " |-- f: float (nullable = true)",
            "",
            "+---+---+---+---+-------+---------+",
            "|  a|  b|  c|  d|      e|        f|",
            "+---+---+---+---+-------+---------+",
            "|  1|  a|  2|1.1|   1000|  1111.11|",
            "|  2|  b|  4|2.2|  10000| 22222.22|",
            "|  3|  c|  6|3.3| 100000|333333.34|",
            "|  4|  d|  8|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "",
        ]
        assert result == expected

    def test_display_intermediary_schema_03(self) -> None:
        self.ps_df_formatting.transform(display_intermediary_schema).withColumn(
            "g", F.expr("c * 2").cast("int").cast("string")
        ).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "root",
            " |-- a: long (nullable = true)",
            " |-- b: string (nullable = true)",
            " |-- c: float (nullable = true)",
            " |-- d: float (nullable = true)",
            " |-- e: integer (nullable = true)",
            " |-- f: float (nullable = true)",
            "",
            "+---+---+---+---+-------+---------+---+",
            "|  a|  b|  c|  d|      e|        f|  g|",
            "+---+---+---+---+-------+---------+---+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|  2|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|  4|",
            "|  3|  c|3.0|3.3| 100000|333333.34|  6|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|  8|",
            "+---+---+---+---+-------+---------+---+",
            "",
            "",
        ]
        assert result == expected


## --------------------------------------------------------------------------- #
##  `display_intermediary_columns()`                                        ####
## --------------------------------------------------------------------------- #


class TestDisplayIntermediaryColumns(PySparkSetup, TestCase):

    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys: pytest.CaptureFixture) -> None:
        self.capsys: pytest.CaptureFixture = capsys

    def test_display_intermediary_columns_01(self) -> None:
        self.ps_df_formatting.transform(display_intermediary_columns).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "['a', 'b', 'c', 'd', 'e', 'f']",
            "+---+---+---+---+-------+---------+",
            "|  a|  b|  c|  d|      e|        f|",
            "+---+---+---+---+-------+---------+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|",
            "|  3|  c|3.0|3.3| 100000|333333.34|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|",
            "+---+---+---+---+-------+---------+",
            "",
            "",
        ]
        assert result == expected

    def test_display_intermediary_columns_02(self) -> None:
        self.ps_df_formatting.transform(display_intermediary_columns).withColumn(
            "g", F.expr("c * 2")
        ).show()
        result: str_list = self.capsys.readouterr().out.split("\n")
        expected: str_list = [
            "['a', 'b', 'c', 'd', 'e', 'f']",
            "+---+---+---+---+-------+---------+---+",
            "|  a|  b|  c|  d|      e|        f|  g|",
            "+---+---+---+---+-------+---------+---+",
            "|  1|  a|1.0|1.1|   1000|  1111.11|2.0|",
            "|  2|  b|2.0|2.2|  10000| 22222.22|4.0|",
            "|  3|  c|3.0|3.3| 100000|333333.34|6.0|",
            "|  4|  d|4.0|4.4|1000000|4444444.5|8.0|",
            "+---+---+---+---+-------+---------+---+",
            "",
            "",
        ]
        assert result == expected
