# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from itertools import product

# ## Python Third Party Imports ----
import pytest
from chispa.dataframe_comparer import assert_df_equality
from parameterized import parameterized
from pyspark.sql import DataFrame as psDataFrame, functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.scale import DEFAULT_DECIMAL_ACCURACY, round_column, round_columns


## --------------------------------------------------------------------------- #
##  Initialisation                                                          ####
## --------------------------------------------------------------------------- #


def setUpModule() -> None:
    PySparkSetup.set_up()


def tearDownModule() -> None:
    PySparkSetup.tear_down()


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestSchemas                                                              ####
# ---------------------------------------------------------------------------- #


class TestRoundColumnToDecimalScale(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_round_column_1(self) -> None:
        result: psDataFrame = round_column(
            dataframe=self.ps_df_decimals,
            column="b",
        )
        expected: psDataFrame = self.ps_df_decimals.withColumn(
            colName="b",
            col=F.round("b", DEFAULT_DECIMAL_ACCURACY),
        )
        assert_df_equality(result, expected)

    def test_round_column_2(self) -> None:
        result: psDataFrame = round_column(
            dataframe=self.ps_df_decimals,
            column="c",
        )
        expected: psDataFrame = self.ps_df_decimals.withColumn(
            colName="c",
            col=F.round("c", DEFAULT_DECIMAL_ACCURACY),
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=list(product(["scale"], range(20))),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(prm) for prm in params[0]])}",
    )
    def test_round_column_3(self, name, scale) -> None:
        result: psDataFrame = round_column(
            dataframe=self.ps_df_decimals,
            column="b",
            scale=scale,
        )
        expected: psDataFrame = self.ps_df_decimals.withColumn(
            colName="b",
            col=F.round(col="b", scale=scale),
        )
        assert_df_equality(result, expected)

    def test_round_column_4(self) -> None:
        with pytest.raises(TypeError) as e:
            _ = round_column(
                dataframe=self.ps_df_decimals,
                column="a",
            )
        expected_message = (
            "Column is not the correct type. Please check.\n"
            "For column 'a', the type is 'bigint'.\n"
            "In order to round it, it needs to be one of: '['float', 'double', 'decimal']'."
        )
        assert str(e.value) == expected_message


class TestRoundColumnsToDecimalScale(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_round_columns_1(self) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, DEFAULT_DECIMAL_ACCURACY) for col in ["b", "c"]}
        )
        assert_df_equality(result, expected)

    def test_round_columns_2(self) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            columns=["b", "c"],
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, DEFAULT_DECIMAL_ACCURACY) for col in ["b", "c"]}
        )
        assert_df_equality(result, expected)

    def test_round_columns_3(self) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            columns=["b"],
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, DEFAULT_DECIMAL_ACCURACY) for col in ["b"]}
        )
        assert_df_equality(result, expected)

    def test_round_columns_4(self) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            columns="b",
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, DEFAULT_DECIMAL_ACCURACY) for col in ["b"]}
        )
        assert_df_equality(result, expected)

    def test_round_columns_5(self) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            scale=5,
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, 5) for col in ["b", "c"]}
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=list(product(["columns"], ["all", "all_float"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join(params[0])}",
    )
    def test_round_columns_6(self, name, columns) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            columns=columns,
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, DEFAULT_DECIMAL_ACCURACY) for col in ["b", "c"]}
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=list(product(["scale"], range(20))),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(prm) for prm in params[0]])}",
    )
    def test_round_columns_7(self, name, scale) -> None:
        result: psDataFrame = round_columns(
            dataframe=self.ps_df_decimals,
            scale=scale,
        )
        expected: psDataFrame = self.ps_df_decimals.withColumns(
            {col: F.round(col, scale) for col in ["b", "c"]}
        )
        assert_df_equality(result, expected)

    def test_round_columns_8(self) -> None:
        with pytest.raises(TypeError) as e:
            _ = round_columns(
                dataframe=self.ps_df_decimals,
                columns=["a", "b"],
            )
        expected_message = (
            "Columns are not the correct types. Please check.\n"
            "These columns are invalid: '[('a', 'bigint')]'.\n"
            "In order to round them, they need to be one of: '['float', 'double', 'decimal']'."
        )
        assert str(e.value) == expected_message
