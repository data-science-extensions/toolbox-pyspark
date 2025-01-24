# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
import numpy as np
from chispa import assert_df_equality
from pandas import DataFrame as pdDataFrame
from pyspark.sql import functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.dimensions import (
    get_dims,
    get_dims_of_tables,
    make_dimension_table,
    replace_columns_with_dimension_id,
)
from toolbox_pyspark.types import cast_column_to_type


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
#  TestGetDims                                                              ####
# ---------------------------------------------------------------------------- #


class TestGetDims(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_get_dims_1(self) -> None:
        result: dict[str, str] = get_dims(self.ps_df)
        expected: dict[str, str] = {"rows": "4", "cols": "2"}
        assert result == expected

    def test_get_dims_2(self) -> None:
        result: dict[str, int] = get_dims(self.ps_df, use_comma=False)
        expected: dict[str, int] = {"rows": 4, "cols": 2}
        assert result == expected

    def test_get_dims_3(self) -> None:
        result: dict[str, str] = get_dims(self.ps_df, use_comma=True)
        expected: dict[str, str] = {"rows": "4", "cols": "2"}
        assert result == expected

    def test_get_dims_4(self) -> None:
        result: tuple[str, str] = get_dims(self.ps_df, use_comma=True, use_names=False)
        expected: tuple[str, str] = ("4", "2")
        assert result == expected

    def test_get_dims_4(self) -> None:
        result: tuple[int, int] = get_dims(self.ps_df, use_comma=False, use_names=False)
        expected: tuple[int, int] = (4, 2)
        assert result == expected


class TestGetSizesOfListOfTables(PySparkSetup):

    def test_get_dims_of_tables_1(self) -> None:

        # Set up the data
        df1 = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "a": range(5000),
                    "b": range(5000),
                }
            )
        )
        df2 = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "a": range(10000),
                    "b": range(10000),
                    "c": range(10000),
                }
            )
        )
        df3_prd = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "a": range(1000),
                    "b": range(1000),
                    "c": range(1000),
                    "d": range(1000),
                }
            )
        )

        # Test use_comma=True
        result1 = get_dims_of_tables(
            ["df1", "df2", "df3_prd", "df_4"], scope=locals(), use_comma=True
        )
        expected1 = pdDataFrame(
            {
                "table": ["df1", "df2", "df3", "df_4"],
                "type": ["", "", "prd", ""],
                "rows": ["5,000", "10,000", "1,000", "Did not load"],
                "cols": ["2", "3", "4", "Did not load"],
            }
        )
        assert result1.to_dict(orient="list") == expected1.to_dict(orient="list")

        # Test use_comma=False
        result2 = get_dims_of_tables(
            ["df1", "df2", "df3_prd", "df_4"], scope=locals(), use_comma=False
        ).to_dict(orient="list")
        expected2 = pdDataFrame(
            {
                "table": ["df1", "df2", "df3", "df_4"],
                "type": ["", "", "prd", ""],
                "rows": [5000, 10000, 1000, np.nan],
                "cols": [2, 3, 4, np.nan],
            }
        ).to_dict(orient="list")
        assert str(result2) == str(expected2)


class TestMakeDimensions(PySparkSetup):

    def setUp(self) -> None:
        pass

    def test_make_dimension_table_single_column_1(self) -> None:
        result = make_dimension_table(self.ps_df_dimensions, "d")
        expected = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "id_d": [1, 2],
                    "d": ["a", "b"],
                }
            ).astype({"id_d": "int"})
        ).transform(cast_column_to_type, "id_d", "int")
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_make_dimension_table_single_column_2(self) -> None:
        result = self.ps_df_dimensions.transform(make_dimension_table, "e")
        expected = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "id_e": [1, 2, 3],
                    "e": ["x", "y", "z"],
                }
            )
        ).transform(cast_column_to_type, "id_e", "int")
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_make_dimension_table_multiple_columns(self) -> None:
        result = make_dimension_table(self.ps_df_dimensions, ["c", "d"])
        expected = self.spark.createDataFrame(
            pdDataFrame(
                {
                    "id": [1, 2, 3],
                    "c": ["1", "1", "2"],
                    "d": ["a", "b", "b"],
                }
            )
        ).transform(cast_column_to_type, "id", "int")
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_replace_columns_with_dimension_id_single_column_1(self) -> None:
        dim_table = make_dimension_table(self.ps_df_dimensions, "d")
        result = replace_columns_with_dimension_id(self.ps_df_dimensions, dim_table, "d")
        expected = (
            self.ps_df_dimensions.withColumnRenamed("d", "id_d")
            .withColumn("new", self.add_column_from_list("a", F.lit([1, 2, 2, 2])))
            .withColumn("id_d", F.col("new").cast("int"))
            .drop("new")
        )
        assert_df_equality(result, expected, ignore_nullable=True, ignore_row_order=True)

    def test_replace_columns_with_dimension_id_single_column_2(self) -> None:
        dim_table = make_dimension_table(self.ps_df_dimensions, "e")
        result = self.ps_df_dimensions.transform(
            replace_columns_with_dimension_id, dim_table, "e"
        )
        expected = (
            self.ps_df_dimensions.withColumnRenamed("e", "id_e")
            .withColumn("new", self.add_column_from_list("a", F.lit([1, 1, 2, 3])))
            .withColumn("id_e", F.col("new").cast("int"))
            .drop("new")
        )
        assert_df_equality(result, expected, ignore_nullable=True, ignore_row_order=True)

    def test_replace_columns_with_dimension_id_multiple_columns(self) -> None:
        dim_table = make_dimension_table(self.ps_df_dimensions, ("c", "d"))
        result = replace_columns_with_dimension_id(
            self.ps_df_dimensions, dim_table, ("c", "d")
        )
        expected = self.ps_df_dimensions.withColumn(
            "id", self.add_column_from_list("a", F.lit([1, 2, 3, 3])).cast("int")
        ).select("a", "b", "id", "e")
        assert_df_equality(result, expected, ignore_nullable=True, ignore_row_order=True)
