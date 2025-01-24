# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
import shutil
from pathlib import Path
from typing import Literal, Optional, Union

# ## Python Third Party Imports ----
import pytest
from parameterized import parameterized
from toolbox_python.checkers import is_type
from toolbox_python.collection_types import str_collection, str_list

# ## Local First Party Imports ----
from tests.setup import PySparkSetup, name_func_flat_list, name_func_predefined_name
from toolbox_pyspark.checks import (
    assert_column_exists,
    assert_column_is_type,
    assert_columns_are_type,
    assert_columns_exists,
    assert_table_exists,
    assert_valid_spark_type,
    column_contains_value,
    column_exists,
    column_is_type,
    columns_are_type,
    columns_exists,
    is_vaid_spark_type,
    table_exists,
    warn_column_invalid_type,
    warn_column_missing,
    warn_columns_invalid_type,
    warn_columns_missing,
)
from toolbox_pyspark.constants import VALID_PYSPARK_TYPE_NAMES
from toolbox_pyspark.io import write_to_path
from toolbox_pyspark.utils.exceptions import (
    ColumnDoesNotExistError,
    InvalidPySparkDataTypeError,
    TableDoesNotExistError,
)
from toolbox_pyspark.utils.warnings import (
    ColumnDoesNotExistWarning,
    InvalidPySparkDataTypeWarning,
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
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Column Existence                                                         ####
# ---------------------------------------------------------------------------- #


class TestColumnExistence(PySparkSetup):

    def setUp(self) -> None:
        pass

    @parameterized.expand(
        input=(
            ("exists", True, "a"),
            ("missing", False, "z"),
            ("exists_ignorecase", True, "a", False),
            ("exists_matchcase", False, "A", True),
        ),
        name_func=name_func_predefined_name,
    )
    def test_column_exists(
        self,
        test_name: str,
        expected: bool,
        col: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if match_case:
            result: bool = column_exists(self.ps_df_extended, col, match_case)
        else:
            result: bool = column_exists(self.ps_df_extended, col)
        assert result == expected

    @parameterized.expand(
        input=(
            ("exists", True, ["a", "b"]),
            ("missing", False, ["b", "z"]),
            ("missing_2", False, ["b", "z", "x"]),
            ("exists_ignorecase", True, ["A", "B"], False),
            ("exists_matchcase", False, ["A", "B"], True),
        ),
        name_func=name_func_predefined_name,
    )
    def test_columns_exist(
        self,
        test_name: str,
        expected: bool,
        cols: str_list,
        match_case: Optional[bool] = None,
    ) -> None:
        if match_case:
            result: bool = columns_exists(self.ps_df_extended, cols, match_case)
        else:
            result: bool = columns_exists(self.ps_df_extended, cols)
        assert result == expected

    @parameterized.expand(
        input=(
            ("exists", None, "a"),
            ("missing", "raises", "z"),
            ("exists_matchcase", "raises", "A", True),
            ("exists_matchcase_2", None, "a", True),
            ("exists_ignorecase", None, "A", False),
        ),
        name_func=name_func_predefined_name,
    )
    def test_assert_column_exists(
        self,
        test_name: str,
        expected: Optional[Literal["raises"]],
        col: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "raises" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                assert_column_exists(self.ps_df_extended, col, match_case)
        elif expected == "raises":
            with pytest.raises(ColumnDoesNotExistError):
                assert_column_exists(self.ps_df_extended, col)
        elif match_case:
            assert assert_column_exists(self.ps_df_extended, col, match_case) is None
        else:
            assert assert_column_exists(self.ps_df_extended, col) is None

    @parameterized.expand(
        input=(
            ("exists", None, ["a", "b"]),
            ("missing", "raises", ["b", "z"]),
            ("missing_2", "raises", ["b", "z", "x"]),
            ("exists_matchcase", "raises", ["A", "B"], True),
            ("exists_matchcase_2", None, ["a", "b"], True),
            ("exists_ignorecase", None, ["A", "B"], False),
        ),
        name_func=name_func_predefined_name,
    )
    def test_assert_columns_exists(
        self,
        test_name: str,
        expected: Optional[Literal["raises"]],
        cols: str_collection,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "raises" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                assert_columns_exists(self.ps_df_extended, cols, match_case)
        elif expected == "raises":
            with pytest.raises(ColumnDoesNotExistError):
                assert_columns_exists(self.ps_df_extended, cols)
        elif match_case:
            assert assert_columns_exists(self.ps_df_extended, cols, match_case) is None
        else:
            assert assert_columns_exists(self.ps_df_extended, cols) is None

    @parameterized.expand(
        input=(
            ("exists", None, "a"),
            ("missing", "warns", "z"),
            ("exists_matchcase", "warns", "A", True),
            ("exists_matchcase_2", None, "a", True),
            ("exists_ignorecase", None, "A", False),
        ),
        name_func=name_func_predefined_name,
    )
    def test_warn_column_missing(
        self,
        test_name: str,
        expected: Optional[Literal["warns"]],
        col: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "warns" and match_case:
            with pytest.warns(ColumnDoesNotExistWarning):
                warn_column_missing(self.ps_df_extended, col, match_case)
        elif expected == "warns":
            with pytest.warns(ColumnDoesNotExistWarning):
                warn_column_missing(self.ps_df_extended, col)
        elif match_case:
            assert warn_column_missing(self.ps_df_extended, col, match_case) is None
        else:
            assert warn_column_missing(self.ps_df_extended, col) is None

    @parameterized.expand(
        input=(
            ("exists", None, ["a", "b"]),
            ("missing", "warns", ["b", "z"]),
            ("missing_2", "warns", ["b", "z", "x"]),
            ("exists_matchcase", "warns", ["A", "B"], True),
            ("exists_matchcase_2", None, ["a", "b"], True),
            ("exists_ignorecase", None, ["A", "B"], False),
        ),
        name_func=name_func_predefined_name,
    )
    def test_warn_columns_missing(
        self,
        test_name: str,
        expected: Optional[Literal["warns"]],
        cols: str_collection,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "warns" and match_case:
            with pytest.warns(ColumnDoesNotExistWarning):
                warn_columns_missing(self.ps_df_extended, cols, match_case)
        elif expected == "warns":
            with pytest.warns(ColumnDoesNotExistWarning):
                warn_columns_missing(self.ps_df_extended, cols)
        elif match_case:
            assert warn_columns_missing(self.ps_df_extended, cols, match_case) is None
        else:
            assert warn_columns_missing(self.ps_df_extended, cols) is None


# ---------------------------------------------------------------------------- #
#  Column Contains Value                                                    ####
# ---------------------------------------------------------------------------- #


class TestColumnContainsValue(PySparkSetup):

    def setUp(self) -> None:
        pass

    @parameterized.expand(
        input=(
            ("value_exists", True, "b", "a"),
            ("value_missing", False, "b", "z"),
            ("value_exists_ignorecase", True, "b", "A", False),
            ("value_exists_matchcase", False, "b", "A", True),
            ("column_missing", "raises", "z", "a", False),
        ),
        name_func=name_func_predefined_name,
    )
    def test_column_contains_value(
        self,
        test_name: str,
        expected: Union[bool, Literal["raises"]],
        column: str,
        value: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "raises":
            with pytest.raises(ColumnDoesNotExistError):
                column_contains_value(self.ps_df_extended, column, value, match_case)
        elif match_case is not None:
            result: bool = column_contains_value(
                self.ps_df_extended, column, value, match_case
            )
            assert result == expected
        else:
            result: bool = column_contains_value(self.ps_df_extended, column, value)
            assert result == expected


# ---------------------------------------------------------------------------- #
#  TestValidPySparkDataType                                                 ####
# ---------------------------------------------------------------------------- #


class TestValidPySparkDataType(PySparkSetup):

    def setUp(self) -> None:
        pass

    @parameterized.expand(
        input=VALID_PYSPARK_TYPE_NAMES,
        name_func=name_func_flat_list,
    )
    def test_is_vaid_spark_type_1(self, typ) -> None:
        assert is_vaid_spark_type(typ) is True

    @parameterized.expand(
        input=("np.ndarray", "pd.DataFrame", "dict"),
        name_func=name_func_flat_list,
    )
    def test_is_vaid_spark_type_2(self, typ) -> None:
        assert is_vaid_spark_type(typ) is False

    @parameterized.expand(
        input=("np.ndarray", "pd.DataFrame", "dict"),
        name_func=name_func_flat_list,
    )
    def test_assert_vaid_spark_type_3(self, typ: str) -> None:
        with pytest.raises(InvalidPySparkDataTypeError):
            assert_valid_spark_type(typ)


# ---------------------------------------------------------------------------- #
#  Type checks                                                              ####
# ---------------------------------------------------------------------------- #


class TestColumnTypes(PySparkSetup):

    def setUp(self) -> None:
        pass

    @parameterized.expand(
        input=(
            ("success_int", True, "c", "int"),
            ("success_string", True, "d", "string"),
            ("success_float", True, "e", "float"),
            ("success_double", True, "f", "double"),
            ("success_date", True, "g", "date"),
            ("success_timestamp", True, "h", "timestamp"),
            ("failure_string", False, "c", "string"),
            ("failure_float", False, "d", "float"),
            ("failure_missingcolumn_matchcase", "raises_column", "C", "string", True),
            ("failure_missingcolumn_ignorecase", "raises_column", "x", "string", False),
            ("failure_invalidtype", "raises_type", "c", "error"),
        ),
        name_func=name_func_predefined_name,
    )
    def test_column_is_type(
        self,
        test_name: str,
        expected: Union[bool, Literal["raises_column", "raises_type"]],
        column: str,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if is_type(expected, bool) and match_case:
            assert column_is_type(self.ps_df_types, column, datatype, match_case) == expected
        elif is_type(expected, bool):
            assert column_is_type(self.ps_df_types, column, datatype) == expected
        elif expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                column_is_type(self.ps_df_types, column, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                column_is_type(self.ps_df_types, column, datatype)
        elif expected == "raises_type" and match_case:
            with pytest.raises(InvalidPySparkDataTypeError):
                column_is_type(self.ps_df_types, column, datatype, match_case)

    @parameterized.expand(
        input=(
            ("success_int", True, "c", "int"),
            ("success_string", True, ["b", "d"], "string"),
            ("success_float", True, ("e",), "float"),
            (
                "failure_missingcolumn_matchcase",
                "raises_column",
                ["b", "C"],
                "string",
                True,
            ),
            (
                "failure_missingcolumn_ignorecase",
                "raises_column",
                ("a", "x"),
                "string",
                False,
            ),
            ("failure_invalidtype", "raises_type", "c", "error"),
        ),
        name_func=name_func_predefined_name,
    )
    def test_columns_are_type(
        self,
        test_name: str,
        expected: Union[bool, Literal["raises_column", "raises_type"]],
        columns: str_collection,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if is_type(expected, bool) and match_case:
            assert (
                columns_are_type(self.ps_df_types, columns, datatype, match_case) == expected
            )
        elif is_type(expected, bool):
            assert columns_are_type(self.ps_df_types, columns, datatype) == expected
        elif expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                columns_are_type(self.ps_df_types, columns, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                columns_are_type(self.ps_df_types, columns, datatype)
        elif expected == "raises_type":
            with pytest.raises(InvalidPySparkDataTypeError):
                columns_are_type(self.ps_df_types, columns, datatype)

    @parameterized.expand(
        input=(
            ("success_int", None, "c", "int"),
            ("success_string", None, "d", "string"),
            ("success_float", None, "e", "float"),
            ("success_double", None, "f", "double"),
            ("success_date", None, "g", "date"),
            ("success_timestamp", None, "h", "timestamp"),
            ("columnerror_missing", "raises_column", "x", "string"),
            ("columnerror_matchcase", "raises_column", "C", "int", True),
            ("success_int_ignorecase", None, "C", "int", False),
            ("typeerror_invalid_ignorecase", "raises_type", "c", "error"),
            ("typeerror_wrongtype_matchcase", "raises_type", "c", "string", True),
        ),
        name_func=name_func_predefined_name,
    )
    def test_assert_column_is_type(
        self,
        test_name: str,
        expected: Optional[Literal["raises_column", "raises_type"]],
        column: str,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                assert_column_is_type(self.ps_df_types, column, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                assert_column_is_type(self.ps_df_types, column, datatype)
        elif expected == "raises_type" and match_case:
            with pytest.raises(InvalidPySparkDataTypeError):
                assert_column_is_type(self.ps_df_types, column, datatype, match_case)
        elif expected == "raises_type":
            with pytest.raises(InvalidPySparkDataTypeError):
                assert_column_is_type(self.ps_df_types, column, datatype)
        elif match_case:
            assert (
                assert_column_is_type(self.ps_df_types, column, datatype, match_case) is None
            )
        else:
            assert assert_column_is_type(self.ps_df_types, column, datatype) is None

    @parameterized.expand(
        input=(
            ("success_bigint", None, "c", "int"),
            ("success_string", None, ["b", "d"], "string"),
            ("success_int", None, ("c",), "int", True),
            ("columnerror_bigint", "raises_column", ["a", "c", "x"], "int"),
            ("columnerror_string", "raises_column", ("b", "D", "x"), "string", True),
            ("typeerror_ignorecase", "raises_type", ("a", "b", "d"), "string"),
            ("typeerror_matchcase", "raises_type", ("a", "c"), "error", True),
        ),
        name_func=name_func_predefined_name,
    )
    def test_assert_columns_are_type(
        self,
        test_name: str,
        expected: Optional[Literal["raises_column", "raises_type"]],
        columns: str_collection,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                assert_columns_are_type(self.ps_df_types, columns, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                assert_columns_are_type(self.ps_df_types, columns, datatype)
        elif expected == "raises_type" and match_case:
            with pytest.raises(InvalidPySparkDataTypeError):
                assert_columns_are_type(self.ps_df_types, columns, datatype, match_case)
        elif expected == "raises_type":
            with pytest.raises(InvalidPySparkDataTypeError):
                assert_columns_are_type(self.ps_df_types, columns, datatype)
        elif match_case:
            assert (
                assert_columns_are_type(self.ps_df_types, columns, datatype, match_case)
                is None
            )
        else:
            assert assert_columns_are_type(self.ps_df_types, columns, datatype) is None

    @parameterized.expand(
        input=(
            ("success_int", None, "c", "int"),
            ("success_string", None, "d", "string"),
            ("success_float", None, "e", "float"),
            ("success_double", None, "f", "double"),
            ("success_date", None, "g", "date"),
            ("success_timestamp", None, "h", "timestamp"),
            ("failure_missingcolumn_matchcase", "raises_column", "C", "string", True),
            ("failure_missingcolumn_ignorecase", "raises_column", "x", "string", False),
            ("failure_invalidtype", "raises_type", "c", "error"),
            ("warning_int", "warns", "c", "string"),
        ),
        name_func=name_func_predefined_name,
    )
    def test_warn_column_invalid_type(
        self,
        test_name: str,
        expected: Optional[Literal["warns", "raises_column", "raises_type"]],
        column: str,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected is None:
            assert warn_column_invalid_type(self.ps_df_types, column, datatype) is None
        elif expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                warn_column_invalid_type(self.ps_df_types, column, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                warn_column_invalid_type(self.ps_df_types, column, datatype)
        elif expected == "raises_type":
            with pytest.raises(InvalidPySparkDataTypeError):
                warn_column_invalid_type(self.ps_df_types, column, datatype)
        elif expected == "warns":
            with pytest.warns(InvalidPySparkDataTypeWarning):
                warn_column_invalid_type(self.ps_df_types, column, datatype)

    @parameterized.expand(
        input=(
            ("success_int", None, "c", "int"),
            ("success_string", None, ["b", "d"], "string"),
            ("success_float", None, ("e",), "float"),
            ("failure_missingcolumn_matchcase", "raises_column", "C", "string", True),
            ("failure_missingcolumn_ignorecase", "raises_column", "x", "string", False),
            ("failure_invalidtype", "raises_type", "c", "error"),
            ("warning_int", "warns", "c", "string"),
        ),
        name_func=name_func_predefined_name,
    )
    def test_warn_columns_invalid_type(
        self,
        test_name: str,
        expected: Optional[Literal["warns", "raises_column", "raises_type"]],
        columns: str_collection,
        datatype: str,
        match_case: Optional[bool] = None,
    ) -> None:
        if expected is None:
            assert warn_columns_invalid_type(self.ps_df_types, columns, datatype) is None
        elif expected == "raises_column" and match_case:
            with pytest.raises(ColumnDoesNotExistError):
                warn_columns_invalid_type(self.ps_df_types, columns, datatype, match_case)
        elif expected == "raises_column":
            with pytest.raises(ColumnDoesNotExistError):
                warn_columns_invalid_type(self.ps_df_types, columns, datatype)
        elif expected == "raises_type":
            with pytest.raises(InvalidPySparkDataTypeError):
                warn_columns_invalid_type(self.ps_df_types, columns, datatype)
        elif expected == "warns":
            with pytest.warns(InvalidPySparkDataTypeWarning):
                warn_columns_invalid_type(self.ps_df_types, columns, datatype)


# ---------------------------------------------------------------------------- #
#  TestTableExists                                                          ####
# ---------------------------------------------------------------------------- #


class TestTableExists(PySparkSetup):
    def setUp(self) -> None:
        self.write_dir_name = "io"
        self.write_path: str = f"./src/tests/{self.write_dir_name}"
        self.table_name = "ps_df"
        self.data_format = "parquet"
        self.clean_up = True
        write_to_path(
            data_frame=self.ps_df,
            name=self.table_name,
            path=self.write_path,
            data_format=self.data_format,
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )

    def tearDown(self) -> None:
        write_dir = Path(self.write_path)
        if self.clean_up and write_dir.exists():
            shutil.rmtree(write_dir)

    def test_table_exists_1(self) -> None:
        result: bool = table_exists(
            name=self.table_name,
            path=self.write_path,
            data_format=self.data_format,
            spark_session=self.spark,
        )
        expected = True
        assert result == expected

    def test_table_exists_2(self) -> None:
        result: bool = table_exists(
            name=f"{self.table_name}_transferred",
            path=self.write_path,
            data_format=self.data_format,
            spark_session=self.spark,
        )
        expected = False
        assert result == expected

    def test_table_exists_3(self) -> None:
        with pytest.raises(TableDoesNotExistError):
            assert_table_exists(
                name=f"{self.table_name}_failure",
                path=self.write_path,
                data_format=self.data_format,
                spark_session=self.spark,
            )
