# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from unittest import TestCase

# ## Python Third Party Imports ----
from chispa.dataframe_comparer import assert_df_equality

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.dimensions import get_dims
from toolbox_pyspark.duplication import duplicate_union_dataframe, union_all


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


class TestDuplicateUnionSingleDataFrameByList(PySparkSetup, TestCase):
    def setUp(self) -> None:
        pass

    def test_duplicate_union_dataframe_1(self) -> None:
        """Basic checks"""
        cols = self.ps_df_duplication.columns
        dims = get_dims(dataframe=self.ps_df_duplication, use_comma=False)
        assert cols == ["a", "b", "c", "d", "n"]
        assert dims == {"rows": 4, "cols": 5}

    def test_duplicate_union_dataframe_2(self) -> None:
        """Duplicate with column existing"""
        loop_list = ["a", "b", "c", "d", "e", "f"]
        new_df = duplicate_union_dataframe(
            dataframe=self.ps_df_duplication,
            by_list=loop_list,
            new_column_name="n",
        )
        cols = new_df.columns
        dims = get_dims(dataframe=new_df, use_comma=False)
        assert cols == ["a", "b", "c", "d", "n"]
        assert dims == {"rows": 4 + 2 * 4, "cols": 5}

    def test_duplicate_union_dataframe_3(self) -> None:
        """Duplicate with column missing"""
        loop_list = ["x", "y", "z"]
        new_df = duplicate_union_dataframe(
            dataframe=self.ps_df_duplication,
            by_list=loop_list,
            new_column_name="m",
        )
        cols = new_df.columns
        dims = get_dims(dataframe=new_df, use_comma=False)
        assert cols == ["a", "b", "c", "d", "n", "m"]
        assert dims == {"rows": 4 * 3, "cols": 6}

    def test_duplicate_union_dataframe_5(self) -> None:
        """Duplicate with column existing using PySpark `.transform()` method"""
        loop_list = ["a", "b", "c", "d", "e", "f"]
        new_df = self.ps_df_duplication.transform(
            func=duplicate_union_dataframe,
            by_list=loop_list,
            new_column_name="n",
        )
        cols = new_df.columns
        dims = get_dims(dataframe=new_df, use_comma=False)
        assert cols == ["a", "b", "c", "d", "n"]
        assert dims == {"rows": 4 + 2 * 4, "cols": 5}

    def test_duplicate_union_dataframe_6(self) -> None:
        """Duplicate with column missing using PySpark `.transform()` method"""
        loop_list = ["x", "y", "z"]
        new_df = self.ps_df_duplication.transform(
            func=duplicate_union_dataframe,
            by_list=loop_list,
            new_column_name="m",
        )
        cols = new_df.columns
        dims = get_dims(dataframe=new_df, use_comma=False)
        assert cols == ["a", "b", "c", "d", "n", "m"]
        assert dims == {"rows": 4 * 3, "cols": 6}


class TestUnionAll(PySparkSetup, TestCase):
    def setUp(self) -> None:
        pass

    def test_union_all_1(self) -> None:
        result = union_all([self.ps_df, self.ps_df, self.ps_df])
        expected = self.ps_df.unionByName(self.ps_df).unionByName(self.ps_df)
        assert_df_equality(result, expected)

    def test_union_all_2(self) -> None:
        result = union_all([self.ps_df])
        expected = self.ps_df
        assert_df_equality(result, expected)
