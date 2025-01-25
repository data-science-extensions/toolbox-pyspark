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
import numpy as np
import pandas as pd
import pytest
from typeguard import TypeCheckError

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.cleaning import convert_dataframe
from toolbox_pyspark.info import extract_column_values, get_distinct_values
from toolbox_pyspark.utils.exceptions import ColumnDoesNotExistError


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
#  TestGetColumnValues                                                      ####
# ---------------------------------------------------------------------------- #


class TestGetColumnValues(PySparkSetup, TestCase):

    def setUp(self) -> None:
        pass

    def test_get_column_values_1(self) -> None:
        result = extract_column_values(self.ps_df_extended, "c")
        expected = (
            self.ps_df_extended.select("c").filter("c is not null and c <> ''").distinct()
        )
        expected = convert_dataframe(expected, "pd")
        pd.testing.assert_frame_equal(result, expected)

    def test_get_column_values_2(self) -> None:
        result = extract_column_values(self.ps_df_extended, "c", False)
        expected = self.ps_df_extended.select("c").filter("c is not null and c <> ''")
        expected = convert_dataframe(expected, "pd")
        print(result)
        print(expected)
        pd.testing.assert_frame_equal(result, expected)

    def test_get_column_values_3(self) -> None:
        result = extract_column_values(self.ps_df_extended, "c", False, "flat_list")
        expected = self.ps_df_extended.select("c").filter("c is not null and c <> ''")
        expected = convert_dataframe(expected, "flat_list")
        assert result == expected

    def test_get_column_values_4(self) -> None:
        result = extract_column_values(self.ps_df_extended, "c", False, "np")
        expected = self.ps_df_extended.select("c").filter("c is not null and c <> ''")
        expected = convert_dataframe(expected, "np")
        np.testing.assert_array_equal(result, expected)

    def test_get_column_values_5(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            result = extract_column_values(self.ps_df_extended, "123")

    def test_get_column_values_6(self) -> None:
        with pytest.raises(TypeCheckError):
            result = extract_column_values(self.ps_df_extended, "c", False, "123")


class TestGetDistinctValues(PySparkSetup, TestCase):

    def setUp(self) -> None:
        pass

    def test_get_distinct_values_1(self) -> None:
        result = get_distinct_values(self.ps_df_extended, "b")
        expected = ("a", "b", "c", "d")
        assert sorted(result) == sorted(expected)

    def test_get_distinct_values_2(self) -> None:
        result = get_distinct_values(self.ps_df_extended, "c")
        expected = ("c",)
        assert result == expected

    def test_get_distinct_values_3(self) -> None:
        result = get_distinct_values(self.ps_df_extended, ("c", "d"))
        expected = (("c", "d"),)
        assert result == expected

    def test_get_distinct_values_4(self) -> None:
        result = get_distinct_values(self.ps_df_dimensions, ("c", "d"))
        expected = (("1", "a"), ("1", "b"), ("2", "b"))
        assert sorted(result) == sorted(expected)
