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
from pandas import DataFrame as pdDataFrame
from parameterized import parameterized
from pyspark.sql import DataFrame as psDataFrame

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.columns import (
    delete_columns,
    get_columns,
    get_columns_by_likeness,
    rename_columns,
    reorder_columns,
)
from toolbox_pyspark.utils.exceptions import ColumnDoesNotExistError
from toolbox_pyspark.utils.warnings import ColumnDoesNotExistWarning


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  get_columns                                                              ####
# ---------------------------------------------------------------------------- #


class TestGetColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_get_columns_01(self) -> None:
        """Basic check"""
        result = self.spark.createDataFrame(
            pdDataFrame(self.ps_df_types.dtypes, columns=["col_name", "col_type"])
        )
        expected = self.spark.createDataFrame(self.pd_type_check)
        assert_df_equality(result, expected)

    def test_get_columns_02(self) -> None:
        """Default config"""
        result = get_columns(self.ps_df_types)
        expected = self.ps_df_types.columns
        assert result == expected

    def test_get_columns_03(self) -> None:
        """Specific columns"""
        result = get_columns(self.ps_df_types, ["a", "b", "c"])
        expected = ["a", "b", "c"]
        assert result == expected

    def test_get_columns_04(self) -> None:
        """Single column as list"""
        result = get_columns(self.ps_df_types, ["a"])
        expected = ["a"]
        assert result == expected

    def test_get_columns_05(self) -> None:
        """Single column as str"""
        result = get_columns(self.ps_df_types, "a")
        expected = ["a"]
        assert result == expected

    def test_get_columns_06(self) -> None:
        """All columns"""
        result = get_columns(self.ps_df_types, "all")
        expected = self.ps_df_types.columns
        assert result == expected

    def test_get_columns_07(self) -> None:
        """All string"""
        result = get_columns(self.ps_df_types, "all_str")
        expected = ["b", "d"]
        assert result == expected

    def test_get_columns_08(self) -> None:
        """All int"""
        result = get_columns(self.ps_df_types, "all_int")
        expected = ["c"]
        assert result == expected

    def test_get_columns_09(self) -> None:
        """All float"""
        result = get_columns(self.ps_df_types, "all_decimal")
        expected = ["e", "f"]
        assert result == expected

    def test_get_columns_10(self) -> None:
        """All numeric"""
        result = get_columns(self.ps_df_types, "all_numeric")
        expected = ["c", "e", "f"]
        assert result == expected

    def test_get_columns_11(self) -> None:
        """All date"""
        result = get_columns(self.ps_df_types, "all_date")
        expected = ["g"]
        assert result == expected

    def test_get_columns_12(self) -> None:
        """All datetime"""
        result = get_columns(self.ps_df_types, "all_datetime")
        expected = ["h"]
        assert result == expected

    def test_get_columns_13(self) -> None:
        """All timestamp"""
        result = get_columns(self.ps_df_types, "all_timestamp")
        expected = ["h"]
        assert result == expected


# ---------------------------------------------------------------------------- #
#  get_column_by_likeness                                                   ####
# ---------------------------------------------------------------------------- #


class TestGetColumnsByLikeness(PySparkSetup):
    def setUp(self) -> None:
        pass

    @property
    def df(self) -> psDataFrame:
        values = list(range(1, 6))
        return self.spark.createDataFrame(
            pdDataFrame(
                {
                    "aaa": values,
                    "aab": values,
                    "aac": values,
                    "afa": values,
                    "afb": values,
                    "afc": values,
                    "bac": values,
                }
            )
        )

    def test_get_column_by_likeness__simple(self) -> None:
        result = get_columns_by_likeness(self.df)
        expected = [col.upper() for col in self.df.columns]
        assert result == expected

    @parameterized.expand(
        input=["a", "b", "c"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}",
    )
    def test_get_column_by_likeness__startswith(self, starts_with: str) -> None:
        result = get_columns_by_likeness(self.df, starts_with=starts_with)
        expected = [
            col.upper() for col in self.df.columns if col.startswith(starts_with)
        ]
        assert result == expected

    @parameterized.expand(
        input=["a", "b", "c"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}",
    )
    def test_get_column_by_likeness__contains(self, contains: str) -> None:
        result = get_columns_by_likeness(self.df, contains=contains)
        expected = [col.upper() for col in self.df.columns if contains in col]
        assert result == expected

    @parameterized.expand(
        input=["a", "b", "c"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}",
    )
    def test_get_column_by_likeness__endswith(self, ends_with: str) -> None:
        result = get_columns_by_likeness(self.df, ends_with=ends_with)
        expected = [col.upper() for col in self.df.columns if col.endswith(ends_with)]
        assert result == expected

    @parameterized.expand(
        input=list(product(["a", "b", "c"], ["a", "b", "c"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}",
    )
    def test_get_column_by_likeness__startswith_contains(
        self, starts_with: str, contains: str
    ) -> None:
        result = get_columns_by_likeness(
            self.df,
            starts_with=starts_with,
            contains=contains,
        )
        expected = [
            col.upper()
            for col in self.df.columns
            if col.startswith(starts_with) and contains in col
        ]
        assert result == expected

    @parameterized.expand(
        input=list(product(["a", "b", "c"], ["a", "b", "c"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}",
    )
    def test_get_column_by_likeness__startswith_endswith(
        self, starts_with: str, ends_with: str
    ) -> None:
        result = get_columns_by_likeness(
            self.df,
            starts_with=starts_with,
            ends_with=ends_with,
        )
        expected = [
            col.upper()
            for col in self.df.columns
            if col.startswith(starts_with) and col.endswith(ends_with)
        ]
        assert result == expected

    @parameterized.expand(
        input=list(product(["a", "b", "c"], ["a", "b", "c"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}",
    )
    def test_get_column_by_likeness__contains_endswith(
        self, contains: str, ends_with: str
    ) -> None:
        result = get_columns_by_likeness(
            self.df,
            contains=contains,
            ends_with=ends_with,
        )
        expected = [
            col.upper()
            for col in self.df.columns
            if contains in col and col.endswith(ends_with)
        ]
        assert result == expected

    @parameterized.expand(
        input=list(product(["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}_{params[0][2]}",
    )
    def test_get_column_by_likeness__startswith_contains_endswith(
        self, starts_with: str, contains: str, ends_with: str
    ) -> None:
        result = get_columns_by_likeness(
            self.df,
            starts_with=starts_with,
            contains=contains,
            ends_with=ends_with,
        )
        expected = [
            col.upper()
            for col in self.df.columns
            if col.startswith(starts_with)
            and contains in col
            and col.endswith(ends_with)
        ]
        assert result == expected

    @parameterized.expand(
        input=list(product(["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"])),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}_{params[0][2]}",
    )
    def test_get_column_by_likeness_9(
        self, starts_with: str, contains: str, ends_with: str
    ) -> None:
        result = get_columns_by_likeness(
            self.df,
            starts_with=starts_with,
            contains=contains,
            ends_with=ends_with,
            match_case=True,
        )
        expected = [
            col
            for col in self.df.columns
            if col.startswith(starts_with)
            and contains in col
            and col.endswith(ends_with)
        ]
        assert result == expected

    @parameterized.expand(
        input=list(
            product(
                ["a", "b", "c"],
                ["a", "b", "c"],
                ["a", "b", "c"],
                ["and", "or", "and not", "or not"],
            )
        ),
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{params[0][0]}_{params[0][1]}_{params[0][2]}_{params[0][3].replace(' ','')}",
    )
    def test_get_column_by_likeness__startswith_contains_endswith_operator(
        self,
        starts_with: str,
        contains: str,
        ends_with: str,
        operator: str,
    ) -> None:
        _op = operator
        _ops = {
            "and": lambda x, y: x and y,
            "or": lambda x, y: x or y,
            "and not": lambda x, y: x and not y,
            "or not": lambda x, y: x or not y,
        }
        result = get_columns_by_likeness(
            self.df,
            starts_with=starts_with,
            contains=contains,
            ends_with=ends_with,
            match_case=True,
            operator=operator,
        )
        expected = [
            col
            for col in self.df.columns
            if _ops[_op](
                _ops[_op](col.startswith(starts_with), contains in col),
                col.endswith(ends_with),
            )
        ]
        assert result == expected


# ---------------------------------------------------------------------------- #
#  rename_columns                                                           ####
# ---------------------------------------------------------------------------- #


class TestRenameColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_rename_columns_1(self) -> None:
        """Initial test"""
        result = self.ps_df_extended
        expected = ["a", "b", "c", "d"]
        assert result.columns == expected

    def test_rename_columns_2(self) -> None:
        """Function over single column"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns="a",
        ).columns
        expected = ["A", "b", "c", "d"]
        assert result == expected

    def test_rename_columns_3(self) -> None:
        """Single column, simple function"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns="b",
            string_function="upper",
        ).columns
        expected = ["a", "B", "c", "d"]
        assert result == expected

    def test_rename_columns_4(self) -> None:
        """Single column, complex function"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns="b",
            string_function="replace('b','test')",
        ).columns
        expected = ["a", "test", "c", "d"]
        assert result == expected

    def test_rename_columns_5(self) -> None:
        """Function over list of columns"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns=["a", "b"],
        ).columns
        expected = ["A", "B", "c", "d"]
        assert result == expected

    def test_rename_columns_6(self) -> None:
        """Function over single-element long list"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns=["a"],
        ).columns
        expected = ["A", "b", "c", "d"]
        assert result == expected

    def test_rename_columns_7(self) -> None:
        """Default over all columns"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
        ).columns
        expected = ["A", "B", "C", "D"]
        assert result == expected

    def test_rename_columns_8(self) -> None:
        """Complex function over list of columns"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns=["a", "b"],
            string_function="replace('b','test')",
        ).columns
        expected = ["a", "test", "c", "d"]
        assert result == expected

    @parameterized.expand(
        input=[None, "all"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(param) for param in params[0]])}",
    )
    def test_rename_columns_9(self, columns) -> None:
        """Default over all columns"""
        result = rename_columns(
            dataframe=self.ps_df_extended,
            columns=columns,
        ).columns
        expected = ["A", "B", "C", "D"]
        assert result == expected


# ---------------------------------------------------------------------------- #
#  reorder_columns                                                          ####
# ---------------------------------------------------------------------------- #


class TestReorderColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_reorder_columns_1(self) -> None:
        result = self.ps_df_with_keys.toPandas().to_dict(orient="list")
        expected = {
            "a": list(range(4)),
            "b": ["a", "b", "c", "d"],
            "key_a": list(range(4)),
            "c": ["1"] * 4,
            "d": ["2"] * 4,
            "key_c": ["1"] * 4,
            "key_e": ["3"] * 4,
        }
        assert result == expected

    def test_reorder_columns_2(self) -> None:
        """Default config"""
        result = self.ps_df_with_keys.transform(reorder_columns)
        expected = self.ps_df_with_keys.select(
            [col for col in self.ps_df_with_keys.columns if col.startswith("key")]
            + [col for col in self.ps_df_with_keys.columns if not col.startswith("key")]
        )
        assert_df_equality(result, expected)

    def test_reorder_columns_3(self) -> None:
        """Custom order"""
        new_order = ["key_a", "key_c", "key_e", "b", "a", "c", "d"]
        result = self.ps_df_with_keys.transform(reorder_columns, new_order=new_order)
        expected = self.ps_df_with_keys.select(new_order)
        assert_df_equality(result, expected)

    def test_reorder_columns_4(self) -> None:
        """Custom order, include missing columns"""
        new_order = ["key_a", "key_c", "a", "b"]
        result = self.ps_df_with_keys.transform(
            reorder_columns, new_order=new_order, missing_columns_last=True
        )
        expected = self.ps_df_with_keys.select(
            new_order
            + [col for col in self.ps_df_with_keys.columns if col not in new_order]
        )
        assert_df_equality(result, expected)

    def test_reorder_columns_5(self) -> None:
        """Custom order, exclude missing columns"""
        new_order = ["key_a", "key_c", "a", "b"]
        result = self.ps_df_with_keys.transform(
            reorder_columns, new_order=new_order, missing_columns_last=False
        )
        expected = self.ps_df_with_keys.select(new_order)
        assert_df_equality(result, expected)

    def test_reorder_columns_6(self) -> None:
        """Key columns last"""
        result = self.ps_df_with_keys.transform(
            reorder_columns, key_columns_position="last"
        )
        expected = self.ps_df_with_keys.select(
            [col for col in self.ps_df_with_keys.columns if not col.startswith("key")]
            + [col for col in self.ps_df_with_keys.columns if col.startswith("key")]
        )
        assert_df_equality(result, expected)

    def test_reorder_columns_7(self) -> None:
        """Key columns first"""
        result = self.ps_df_with_keys.transform(
            reorder_columns, key_columns_position="first"
        )
        expected = self.ps_df_with_keys.select(
            [col for col in self.ps_df_with_keys.columns if col.startswith("key")]
            + [col for col in self.ps_df_with_keys.columns if not col.startswith("key")]
        )
        assert_df_equality(result, expected)

    def test_reorder_columns_8(self) -> None:
        """Key columns any order"""
        result = self.ps_df_with_keys.transform(
            reorder_columns, key_columns_position=None
        )
        expected = self.ps_df_with_keys
        assert_df_equality(result, expected)


# ---------------------------------------------------------------------------- #
#  delete_columns                                                           ####
# ---------------------------------------------------------------------------- #


class TestDeleteColumns(PySparkSetup):
    def setUp(self) -> None:
        self.df = self.ps_df_extended

    def test_delete_columns_1(self) -> None:
        result = self.df
        expected = self.df
        assert_df_equality(result, expected)

    def test_delete_columns_2(self) -> None:
        """Single column"""
        cols = "a"
        result = self.df.transform(delete_columns, columns=cols)
        expected = self.df.select([col for col in self.df.columns if not col in [cols]])
        assert_df_equality(result, expected)

    def test_delete_columns_3(self) -> None:
        """Multiple columns"""
        cols = ["a", "b"]
        result = self.df.transform(delete_columns, columns=cols)
        expected = self.df.select([col for col in self.df.columns if not col in cols])
        assert_df_equality(result, expected)

    def test_delete_columns_4(self) -> None:
        """Single column missing, raises error"""
        with pytest.raises(ColumnDoesNotExistError):
            self.df.transform(
                delete_columns,
                columns="z",
                missing_column_handler="raise",
            )

    def test_delete_columns_5(self) -> None:
        """Multiple columns, one missing, raises error"""
        with pytest.raises(ColumnDoesNotExistError):
            self.df.transform(
                delete_columns,
                columns=["a", "b", "z"],
                missing_column_handler="raise",
            )

    def test_delete_columns_6(self) -> None:
        """Multiple columns, all missing, raises error"""
        with pytest.raises(ColumnDoesNotExistError):
            self.df.transform(
                delete_columns,
                columns=["x", "y", "z"],
                missing_column_handler="raise",
            )

    def test_delete_columns_7(self) -> None:
        """Single column missing, raises warning"""
        with pytest.warns(ColumnDoesNotExistWarning):
            self.df.transform(
                delete_columns,
                columns="z",
                missing_column_handler="warn",
            )

    def test_delete_columns_8(self) -> None:
        """Multiple columns, one missing, raises warning"""
        with pytest.warns(ColumnDoesNotExistWarning):
            self.df.transform(
                delete_columns,
                columns=["a", "b", "z"],
                missing_column_handler="warn",
            )

    def test_delete_columns_9(self) -> None:
        """Multiple columns, all missing, raises warning"""
        with pytest.warns(ColumnDoesNotExistWarning):
            self.df.transform(
                delete_columns,
                columns=["x", "y", "z"],
                missing_column_handler="warn",
            )

    def test_delete_columns_10(self) -> None:
        """Single column missing, nothing raised"""
        cols = "z"
        result = self.df.transform(
            delete_columns,
            columns=cols,
            missing_column_handler="pass",
        )
        expected = self.df.select([col for col in self.df.columns if col not in [cols]])
        assert_df_equality(result, expected)

    def test_delete_columns_11(self) -> None:
        """Multiple columns, one missing, nothing raised"""
        cols = ["a", "b", "z"]
        result = self.df.transform(
            delete_columns,
            columns=cols,
            missing_column_handler="pass",
        )
        expected = self.df.select([col for col in self.df.columns if col not in cols])
        assert_df_equality(result, expected)

    def test_delete_columns_12(self) -> None:
        """Multiple columns, all missing, nothing raised"""
        cols = ["x", "y", "z"]
        result = self.df.transform(
            delete_columns,
            columns=cols,
            missing_column_handler="pass",
        )
        expected = self.df.select([col for col in self.df.columns if col not in cols])
        assert_df_equality(result, expected)
