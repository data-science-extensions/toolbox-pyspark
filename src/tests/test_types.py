# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
import pytest
from chispa.dataframe_comparer import assert_df_equality
from parameterized import parameterized
from pyspark.errors.exceptions.captured import ParseException
from pyspark.sql import DataFrame as psDataFrame, functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup, name_func_flat_list
from toolbox_pyspark.types import (
    cast_column_to_type,
    cast_columns_to_type,
    get_column_types,
    map_cast_columns_to_type,
)
from toolbox_pyspark.utils.exceptions import ColumnDoesNotExistError


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestDataframeColumnTypes                                                 ####
# ---------------------------------------------------------------------------- #


class TestDataframeColumnTypes(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_get_column_types_1(self) -> None:
        """Pyspark dataframe"""
        result: psDataFrame = get_column_types(self.ps_df_types)
        expected: psDataFrame = self.spark.createDataFrame(self.pd_type_check)
        assert_df_equality(result, expected)

    def test_get_column_types_2(self) -> None:
        """Pandas dataframe"""
        result = get_column_types(self.ps_df_types, "pd").to_dict(orient="list")
        expected = self.pd_type_check.to_dict(orient="list")
        assert result == expected

    def test_get_column_types_3(self) -> None:
        """Raise error"""
        with pytest.raises(ColumnDoesNotExistError):
            get_column_types(self.ps_df_types, "test")


# ---------------------------------------------------------------------------- #
#  TestCastColumnsToType                                                    ####
# ---------------------------------------------------------------------------- #


class TestCastColumnsToType(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_cast_column_to_type_1(self) -> None:
        """Basic check"""
        result = get_column_types(self.ps_df_types)
        expected = self.spark.createDataFrame(self.pd_type_check)
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=["int", "integer"],
        name_func=name_func_flat_list,
    )
    def test_cast_column_to_type_2(self, datatype) -> None:
        result = cast_column_to_type(
            dataframe=self.ps_df_types,
            column="c",
            datatype=datatype,
        )
        expected = self.ps_df_types.withColumn("c", F.col("c").cast("integer"))
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=["str", "string"],
        name_func=name_func_flat_list,
    )
    def test_cast_column_to_type_3(self, datatype) -> None:
        result = cast_column_to_type(
            dataframe=self.ps_df_types,
            column="c",
            datatype=datatype,
        )
        expected = self.ps_df_types.withColumn("c", F.col("c").cast("string"))
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=["bool", "boolean"],
        name_func=name_func_flat_list,
    )
    def test_cast_column_to_type_4(self, datatype) -> None:
        result = cast_column_to_type(
            dataframe=self.ps_df_types,
            column="c",
            datatype=datatype,
        )
        expected = self.ps_df_types.withColumn("c", F.col("c").cast("boolean"))
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=["float", "double"],
        name_func=name_func_flat_list,
    )
    def test_cast_column_to_type_5(self, datatype) -> None:
        result = cast_column_to_type(
            dataframe=self.ps_df_types,
            column="c",
            datatype=datatype,
        )
        expected = self.ps_df_types.withColumn("c", F.col("c").cast(datatype))
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=["foo", "bar"],
        name_func=name_func_flat_list,
    )
    def test_cast_column_to_type_6(self, datatype) -> None:
        with pytest.raises(ParseException):
            cast_column_to_type(
                dataframe=self.ps_df_types,
                column="c",
                datatype=datatype,
            )

    def test_cast_columns_to_type_1(self) -> None:
        """Multiple columns, simple cast"""
        result = cast_columns_to_type(
            dataframe=self.ps_df_types,
            columns=["c", "d"],
            datatype="int",
        )
        expected = self.ps_df_types.withColumns(
            {col: F.col(col).cast("integer") for col in ["c", "d"]}
        )
        assert_df_equality(result, expected)

    def test_cast_columns_to_type_2(self) -> None:
        """Multiple columns, simple cast"""
        result = cast_columns_to_type(
            dataframe=self.ps_df_types,
            columns=["a", "b"],
            datatype="str",
        )
        expected = self.ps_df_types.withColumns(
            {col: F.col(col).cast("string") for col in ["a", "b"]}
        )
        assert_df_equality(result, expected)

    def test_cast_columns_to_type_3(self) -> None:
        """Multiple columns, complex cast from type"""
        result = self.ps_df_types.transform(
            func=cast_columns_to_type,
            columns=["c", "d"],
            datatype=int,
        ).transform(
            func=cast_columns_to_type,
            columns="d",
            datatype=float,
        )
        expected = self.ps_df_types.withColumns(
            {col: F.col(col).cast("long") for col in ["c", "d"]}
        ).withColumn("d", F.col("d").cast("float"))
        assert_df_equality(result, expected)

    def test_cast_columns_to_type_4(self) -> None:
        """Single column, simple cast from type"""
        result = self.ps_df_types.transform(
            func=cast_columns_to_type,
            columns="c",
            datatype="int",
        )
        expected = self.ps_df_types.withColumn("c", F.col("c").cast("integer"))
        assert_df_equality(result, expected)

    def test_cast_columns_to_type_5(self) -> None:
        """Single column, chained operations"""
        result = self.ps_df_types.transform(
            func=cast_columns_to_type,
            columns="a",
            datatype="str",
        ).transform(
            func=cast_columns_to_type,
            columns="c",
            datatype="int",
        )
        expected = self.ps_df_types.withColumns(
            {
                "a": F.col("a").cast("string"),
                "c": F.col("c").cast("integer"),
            }
        )
        assert_df_equality(result, expected)

    def test_cast_columns_to_type_6(self) -> None:
        """Single column, Float type"""
        result = self.ps_df_types.transform(
            func=cast_columns_to_type,
            columns="c",
            datatype="float",
        ).transform(
            func=cast_columns_to_type,
            columns="d",
            datatype=float,
        )
        expected = self.ps_df_types.withColumns(
            {col: F.col(col).cast("float") for col in ["c", "d"]}
        )
        assert_df_equality(result, expected)


# ---------------------------------------------------------------------------- #
#  TestMapCastColumnsToType                                                 ####
# ---------------------------------------------------------------------------- #


class TestMapCastColumnsToType(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_map_cast_columns_to_type_1(self) -> None:
        """Multiple columns, simple cast"""
        result = map_cast_columns_to_type(
            dataframe=self.ps_df_types,
            columns_type_mapping={"int": ["c", "d"], "string": ["a", "b"]},
        ).dtypes
        expected = [
            ("a", "string"),
            ("b", "string"),
            ("c", "int"),
            ("d", "int"),
            ("e", "float"),
            ("f", "double"),
            ("g", "date"),
            ("h", "timestamp"),
        ]
        assert result == expected

    def test_map_cast_columns_to_type_2(self) -> None:
        """Multiple columns, complex cast"""
        result = map_cast_columns_to_type(
            dataframe=self.ps_df_types,
            columns_type_mapping={int: ["c", "d"], str: ["a", "b"]},
        ).dtypes
        expected = [
            ("a", "string"),
            ("b", "string"),
            ("c", "int"),
            ("d", "int"),
            ("e", "float"),
            ("f", "double"),
            ("g", "date"),
            ("h", "timestamp"),
        ]
        assert result == expected

    def test_map_cast_columns_to_type_3(self) -> None:
        """Single columns, simple cast"""
        result = self.ps_df_types.transform(
            func=map_cast_columns_to_type,
            columns_type_mapping={"string": "a", "float": "c"},
        ).dtypes
        expected = [
            ("a", "string"),
            ("b", "string"),
            ("c", "float"),
            ("d", "string"),
            ("e", "float"),
            ("f", "double"),
            ("g", "date"),
            ("h", "timestamp"),
        ]
        assert result == expected

    def test_map_cast_columns_to_type_4(self) -> None:
        """Single columns, simple cast"""
        result = self.ps_df_types.transform(
            func=map_cast_columns_to_type,
            columns_type_mapping={float: "a", "float": "c"},
        ).dtypes
        expected = [
            ("a", "float"),
            ("b", "string"),
            ("c", "float"),
            ("d", "string"),
            ("e", "float"),
            ("f", "double"),
            ("g", "date"),
            ("h", "timestamp"),
        ]
        assert result == expected
