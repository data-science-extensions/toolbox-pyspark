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
from pandas import DataFrame as pdDataFrame

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.dimensions import (
    get_dims,
    get_dims_of_tables,
)


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
