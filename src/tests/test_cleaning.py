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
import numpy as np
import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from parameterized import parameterized
from pyspark.sql import DataFrame as psDataFrame, functions as F, types as T
from toolbox_python.lists import flatten

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.cleaning import (
    apply_function_to_column,
    apply_function_to_columns,
    convert_dataframe,
    create_empty_dataframe,
    drop_matching_rows,
    get_column_values,
    keep_first_record_by_columns,
    trim_spaces_from_column,
    trim_spaces_from_columns,
    update_nullability,
)
from toolbox_pyspark.constants import (
    VALID_LIST_OBJECT_NAMES,
    VALID_NUMPY_ARRAY_NAMES,
    VALID_PANDAS_DATAFRAME_NAMES,
    VALID_PYSPARK_DATAFRAME_NAMES,
    WHITESPACE_CHARACTERS as WHITESPACES,
)


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  create_empty_dataframe                                                   ####
# ---------------------------------------------------------------------------- #


class TestCreateEmptyDataFrame(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_create_empty_dataframe_1(self) -> None:
        assert isinstance(create_empty_dataframe(self.spark), psDataFrame)

    def test_create_empty_dataframe_2(self) -> None:
        assert isinstance(create_empty_dataframe(self.spark).columns, list)

    def test_create_empty_dataframe_3(self) -> None:
        assert len(create_empty_dataframe(self.spark).columns) == 0

    def test_create_empty_dataframe_4(self) -> None:
        assert create_empty_dataframe(self.spark).schema == T.StructType([])


# ---------------------------------------------------------------------------- #
#  keep_first_record_by_columns                                             ####
# ---------------------------------------------------------------------------- #


class TestKeepFirstRecordByColumns(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_keep_first_record_by_columns_1(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, "c")
        expected = self.ps_df_duplicates.where("a in (1, 3)")
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_2(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, "d")
        expected = self.ps_df_duplicates.where("a in (1, 2)")
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_3(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, "e")
        expected = self.ps_df_duplicates.where("a in (1, 3, 4)")
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_4(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, ["c", "d"])
        expected = self.ps_df_duplicates.where("a in (1, 2, 3)")
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_5(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, ["c", "e"])
        expected = self.ps_df_duplicates.where("a in (1, 3, 4)")
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_6(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, ["d", "e"])
        expected = self.ps_df_duplicates
        assert_df_equality(result, expected)

    def test_keep_first_record_by_columns_7(self):
        result = keep_first_record_by_columns(self.ps_df_duplicates, ["c", "d", "e"])
        expected = self.ps_df_duplicates
        assert_df_equality(result, expected)


# ---------------------------------------------------------------------------- #
#  TestDataFrameToType                                                      ####
# ---------------------------------------------------------------------------- #


class TestDataFrameToType(PySparkSetup):

    def setUp(self) -> None:
        pass

    @parameterized.expand(
        input=VALID_PYSPARK_DATAFRAME_NAMES,
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(param) for param in params[0]])}",
    )
    def test_convert_dataframe_1(self, datatype) -> None:
        result = convert_dataframe(self.ps_df, datatype)
        expected = self.ps_df
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=VALID_PANDAS_DATAFRAME_NAMES,
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(param) for param in params[0]])}",
    )
    def test_convert_dataframe_2(self, datatype) -> None:
        result = convert_dataframe(self.ps_df, datatype)
        expected = self.ps_df.toPandas()
        pd.testing.assert_frame_equal(result, expected)

    @parameterized.expand(
        input=VALID_NUMPY_ARRAY_NAMES,
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(param) for param in params[0]])}",
    )
    def test_convert_dataframe_3(self, datatype) -> None:
        result = convert_dataframe(self.ps_df, datatype)
        expected = self.ps_df.toPandas().values
        np.testing.assert_array_equal(result, expected)

    @parameterized.expand(
        input=VALID_LIST_OBJECT_NAMES,
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(param) for param in params[0]])}",
    )
    def test_convert_dataframe_4(self, datatype) -> None:
        result = convert_dataframe(self.ps_df, datatype)
        expected = self.ps_df.toPandas().values.tolist()
        expected = flatten(expected) if "flat" in datatype else expected
        assert result == expected

    def test_convert_dataframe_5(self) -> None:
        with pytest.raises(ValueError):
            convert_dataframe(self.ps_df, "testing")


# ---------------------------------------------------------------------------- #
#  TestGetColumnValues                                                      ####
# ---------------------------------------------------------------------------- #


class TestGetColumnValues(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_get_column_values_1(self) -> None:
        result = get_column_values(self.ps_df_extended, "c")
        expected = (
            self.ps_df_extended.select("c")
            .filter("c is not null and c <> ''")
            .distinct()
        )
        expected = convert_dataframe(expected, "pd")
        pd.testing.assert_frame_equal(result, expected)

    def test_get_column_values_2(self) -> None:
        result = get_column_values(self.ps_df_extended, "c", False)
        expected = self.ps_df_extended.select("c").filter("c is not null and c <> ''")
        expected = convert_dataframe(expected, "pd")
        pd.testing.assert_frame_equal(result, expected)

    def test_get_column_values_3(self) -> None:
        result = get_column_values(self.ps_df_extended, "c", False, "flat_list")
        expected = self.ps_df_extended.select("c").filter("c is not null and c <> ''")
        expected = convert_dataframe(expected, "flat_list")
        assert result == expected


# ---------------------------------------------------------------------------- #
#  TestUpdateNullability                                                    ####
# ---------------------------------------------------------------------------- #


class TestUpdateNullability(PySparkSetup):

    def setUp(self) -> None:
        self.table = self.ps_df_extended

    def test_update_nullability_1(self) -> None:
        columns = "b"
        result = update_nullability(self.table, columns, False)
        schema = self.table.schema
        for field in schema:
            if field.name in list(columns):
                field.nullable = False
        expected = self.spark.createDataFrame(data=self.table.rdd, schema=schema)
        assert_df_equality(result, expected)

    def test_update_nullability_2(self) -> None:
        columns = ["b", "c"]
        result = update_nullability(self.table, columns, False)
        schema = self.table.schema
        for field in schema:
            if field.name in columns:
                field.nullable = False
        expected = self.spark.createDataFrame(data=self.table.rdd, schema=schema)
        assert_df_equality(result, expected)


# ---------------------------------------------------------------------------- #
#  TestTrimSpacesFromListOfColumns                                          ####
# ---------------------------------------------------------------------------- #


class TestTrimSpacesFromListOfColumns(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_trim_spaces_from_columns_1(self) -> None:
        result = self.ps_df_trimming.toPandas().to_dict(orient="list")
        expected = {
            "a": [1, 2, 3, 4],
            "b": ["a", "b", "c", "d"],
            "c": ["1   ", "1   ", "1   ", "1   "],
            "d": ["   2", "   2", "   2", "   2"],
            "e": ["   3   ", "   3   ", "   3   ", "   3   "],
        }
        assert result == expected

    def test_trim_spaces_from_columns_2(self) -> None:
        """Single column"""
        result = trim_spaces_from_column(self.ps_df_trimming, "c")
        expected = self.ps_df_trimming.withColumn("c", F.trim("c"))
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=list(product([str, list], ["b", "c", "d", "e"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0].__name__}_{params[0][1]}",
    )
    def test_trim_spaces_from_columns_3(self, cast_type, column) -> None:
        """Single column, parameterised"""
        result = trim_spaces_from_columns(self.ps_df_trimming, cast_type(column))
        expected = self.ps_df_trimming.withColumn(column, F.trim(column))
        assert_df_equality(result, expected)

    def test_trim_spaces_from_columns_4(self) -> None:
        """Multiple columns"""
        result = trim_spaces_from_columns(self.ps_df_trimming, ["d", "e"])
        expected = self.ps_df_trimming.withColumns(
            {col: F.trim(col) for col in ["d", "e"]}
        )
        assert_df_equality(result, expected)

    def test_trim_spaces_from_columns_5(self) -> None:
        """Default params"""
        result = trim_spaces_from_columns(self.ps_df_trimming)
        expected = self.ps_df_trimming.withColumn(
            "a", F.col("a").cast("string")
        ).withColumns(
            {
                col: F.trim(col)
                for col, typ in self.ps_df_trimming.dtypes
                if typ == "string"
            }
        )
        assert_df_equality(result, expected)

    def test_trim_spaces_from_columns_6(self) -> None:
        """Check all whitespace characters"""
        space_names = WHITESPACES.to_list("name")
        space_chars = WHITESPACES.to_list("chr")
        result = trim_spaces_from_columns(
            self.spark.createDataFrame(
                pd.DataFrame(
                    {
                        "name": space_names,
                        "char": space_chars,
                        "check": [f"a_{char}" for char in space_chars],
                    }
                )
            )
        )
        expected = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "name": space_names,
                    "char": ["" for _ in space_chars],
                    "check": ["a_" for _ in space_chars],
                }
            )
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=[None, "all", "all_string"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(prm) for prm in params[0]])}",
    )
    def test_trim_spaces_from_columns_7(self, columns) -> None:
        """All columns"""
        result = trim_spaces_from_columns(
            self.ps_df_trimming.withColumn("a", F.col("a").cast("string")),
            columns=columns,
        )
        expected = self.ps_df_trimming.withColumn(
            "a", F.col("a").cast("string")
        ).withColumns(
            {
                col: F.trim(col)
                for col, typ in self.ps_df_trimming.dtypes
                if typ == "string"
            }
        )
        assert_df_equality(result, expected)


class TestApplyFunctionToColumn(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_apply_function_to_column_1(self) -> None:
        """Default"""
        result = apply_function_to_column(self.ps_df_extended, "b")
        expected = self.ps_df_extended.withColumn("b", F.upper("b"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_2(self) -> None:
        """Default params"""
        result = apply_function_to_column(self.ps_df_extended, "c")
        expected = self.ps_df_extended.withColumn("c", F.upper("c"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_3(self) -> None:
        """Simple function"""
        result = apply_function_to_column(self.ps_df_extended, "c", "lower")
        expected = self.ps_df_extended.withColumn("c", F.lower("c"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_4(self) -> None:
        """Complex function, using args"""
        result = apply_function_to_column(self.ps_df_extended, "d", "lpad", 5, "?")
        expected = self.ps_df_extended.withColumn("d", F.lpad("d", 5, "?"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_5(self) -> None:
        """Complex func, using kwargs"""
        result = apply_function_to_column(
            dataframe=self.ps_df_extended,
            column="d",
            function="lpad",
            len=5,
            pad="?",
        )
        expected = self.ps_df_extended.withColumn("d", F.lpad("d", 5, "?"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_6(self) -> None:
        """Different complex function, using kwargs"""
        result = apply_function_to_column(
            dataframe=self.ps_df_extended,
            column="b",
            function="regexp_replace",
            pattern="c",
            replacement="17",
        )
        expected = self.ps_df_extended.withColumn("b", F.regexp_replace("b", "c", "17"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_7(self) -> None:
        """Part of pipe"""
        result = self.ps_df_extended.transform(
            func=apply_function_to_column,
            column="d",
            function="lpad",
            len=5,
            pad="?",
        )
        expected = self.ps_df_extended.withColumn("d", F.lpad("d", 5, "?"))
        assert_df_equality(result, expected)

    def test_apply_function_to_column_8(self) -> None:
        """Column name in different case"""
        result = self.ps_df_extended.transform(
            func=apply_function_to_column,
            column="D",
            function="upper",
        )
        expected = self.ps_df_extended.withColumn("D", F.upper("D"))
        assert_df_equality(result, expected)


class TestApplyFunctionToListOfColumns(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_apply_function_to_columns_1(self) -> None:
        """Default"""
        result = apply_function_to_columns(self.ps_df_extended, ["b"])
        expected = self.ps_df_extended.withColumn("b", F.upper("b"))
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_2(self) -> None:
        """Default params"""
        result = apply_function_to_columns(self.ps_df_extended, ["b", "c"])
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.upper("b"),
                "c": F.upper("c"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_3(self) -> None:
        """Simple func"""
        result = apply_function_to_columns(self.ps_df_extended, ["b", "c"], "lower")
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.lower("b"),
                "c": F.lower("c"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_4(self) -> None:
        """Complex func, with args"""
        result = apply_function_to_columns(
            self.ps_df_extended,
            ["b", "c", "d"],
            "lpad",
            5,
            "?",
        )
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.lpad("b", 5, "?"),
                "c": F.lpad("c", 5, "?"),
                "d": F.lpad("d", 5, "?"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_5(self) -> None:
        """Complex func, with kwargs"""
        result = apply_function_to_columns(
            dataframe=self.ps_df_extended,
            columns=["b", "c", "d"],
            function="lpad",
            len=5,
            pad="?",
        )
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.lpad("b", 5, "?"),
                "c": F.lpad("c", 5, "?"),
                "d": F.lpad("d", 5, "?"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_6(self) -> None:
        """Different complex func, with kwargs"""
        result = apply_function_to_columns(
            dataframe=self.ps_df_extended,
            columns=["b", "c", "d"],
            function="regexp_replace",
            pattern="c",
            replacement="17",
        )
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.regexp_replace("b", "c", "17"),
                "c": F.regexp_replace("c", "c", "17"),
                "d": F.regexp_replace("d", "c", "17"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_7(self) -> None:
        """Part of pipe"""
        result = self.ps_df_extended.transform(
            func=apply_function_to_columns,
            columns=["b", "c", "d"],
            function="lpad",
            len=5,
            pad="?",
        )
        expected = self.ps_df_extended.withColumns(
            {
                "b": F.lpad("b", 5, "?"),
                "c": F.lpad("c", 5, "?"),
                "d": F.lpad("d", 5, "?"),
            }
        )
        assert_df_equality(result, expected)

    def test_apply_function_to_columns_8(self) -> None:
        """Column name in different case"""
        result = self.ps_df_extended.transform(
            func=apply_function_to_columns,
            columns=["B", "c", "D"],
            function="upper",
        )
        expected = self.ps_df_extended.withColumns(
            {
                "B": F.upper("B"),
                "c": F.upper("c"),
                "D": F.upper("D"),
            }
        )
        assert_df_equality(result, expected)


class TestCleanOneTableFromAnotherTable(PySparkSetup):

    def setUp(self) -> None:
        self.left_table = self.ps_df_duplication
        self.right_table = self.ps_df_duplication.where("a in ('1','2')")

    def test_drop_matching_rows_1(self) -> None:
        """Single column"""
        result = drop_matching_rows(
            left_table=self.left_table,
            right_table=self.right_table,
            on_keys=["a"],
        )
        expected = self.left_table.where("a not in ('1','2')")
        assert_df_equality(result, expected)

    def test_drop_matching_rows_2(self) -> None:
        """Single column as string"""
        result = drop_matching_rows(
            left_table=self.left_table,
            right_table=self.right_table,
            on_keys="a",
        )
        expected = self.left_table.where("a not in ('1','2')")
        assert_df_equality(result, expected)

    def test_drop_matching_rows_3(self) -> None:
        """Multiple columns"""
        result = drop_matching_rows(
            left_table=self.left_table,
            right_table=self.right_table,
            on_keys=["a", "b"],
        )
        expected = self.left_table.where("a not in ('1','2')")
        assert_df_equality(result, expected)

    def test_drop_matching_rows_4(self) -> None:
        """Including `where` clause"""
        result = self.left_table.transform(
            func=drop_matching_rows,
            right_table=self.right_table,
            on_keys=["a"],
            where_clause="n <> 'd'",
        )
        expected = self.left_table.where("a not in ('1','2') and n <> 'd'")
        assert_df_equality(result, expected)
