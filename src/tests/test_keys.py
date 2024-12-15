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
from typing import Literal

# ## Python Third Party Imports ----
from chispa.dataframe_comparer import assert_df_equality
from parameterized import parameterized
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.collection_types import str_list

# ## Local First Party Imports ----
from tests.setup import PySparkSetup, name_func_nested_list
from toolbox_pyspark.keys import add_key_from_columns, add_keys_from_columns


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestAddKeysFromListsOfColumns                                            ####
# ---------------------------------------------------------------------------- #


class TestAddKeysFromListsOfColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_add_key_from_columns_1(self) -> None:
        result: psDataFrame = add_key_from_columns(
            dataframe=self.ps_df_keys,
            columns=["a", "b"],
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {"key_A_B": F.concat_ws("_", "a", "b")}
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=list(
            product(
                ["list", "tuple"],
                ["list", "tuple", "set"],
                ["list", "tuple", "set"],
            )
        ),
        name_func=name_func_nested_list,
    )
    def test_add_keys_from_columns_1(
        self,
        outer: Literal["list", "tuple", "set"],
        first_inner: Literal["list", "tuple", "set"],
        second_inner: Literal["list", "tuple", "set"],
    ) -> None:
        """Parameterised test of all combination of types for outer collection and two different inner collections."""
        outer_type: type = eval(outer)
        first_inner_type: type = eval(first_inner)
        second_inner_type: type = eval(second_inner)
        first_cols: str_list = ["a", "b"]
        second_cols: str_list = ["b", "c"]
        first = first_inner_type(first_cols)
        second = second_inner_type(second_cols)
        first_key: str = f'key_{"_".join([col.upper() for col in first])}'
        second_key: str = f'key_{"_".join([col.upper() for col in second])}'
        params = outer_type([first, second])
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns=params,
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                first_key: F.concat_ws("_", *first),
                second_key: F.concat_ws("_", *second),
            }
        )
        assert_df_equality(result, expected)

    def test_add_keys_from_columns_2(self) -> None:
        """Test with different `join_character`."""
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns=[
                ("a", "b"),
                ("b", "c"),
            ],
            join_character=".",
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                "key_A_B": F.concat_ws(".", "a", "b"),
                "key_B_C": F.concat_ws(".", "b", "c"),
            }
        )
        assert_df_equality(result, expected)

    def test_add_keys_from_columns_3(self) -> None:
        """Test with `dict`."""
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns={
                "ab_key": ["a", "b"],
                "bc_key": ["b", "c"],
            },
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                "ab_key": F.concat_ws("_", "a", "b"),
                "bc_key": F.concat_ws("_", "b", "c"),
            }
        )
        assert_df_equality(result, expected)

    def test_add_keys_from_columns_4(self) -> None:
        """Test using `dict` and different `join_character`."""
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns={
                "ab_key": ("a", "b"),
                "bc_key": ("b", "c"),
            },
            join_character=".",
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                "ab_key": F.concat_ws(".", "a", "b"),
                "bc_key": F.concat_ws(".", "b", "c"),
            }
        )
        assert_df_equality(result, expected)

    def test_add_keys_from_columns_5(self) -> None:
        """Test using collections with single columns."""
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns=[["a"], "b"],
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                "key_A": F.concat_ws("_", "a"),
                "key_B": F.concat_ws("_", "b"),
            }
        )
        assert_df_equality(result, expected)

    def test_add_keys_from_columns_6(self) -> None:
        """Test using dict with single columns."""
        result: psDataFrame = add_keys_from_columns(
            dataframe=self.ps_df_keys,
            collection_of_columns={"a_key": ["a"], "b_key": "b"},
        )
        expected: psDataFrame = self.ps_df_keys.withColumns(
            {
                "a_key": F.concat_ws("_", "a"),
                "b_key": F.concat_ws("_", "b"),
            }
        )
        assert_df_equality(result, expected)
