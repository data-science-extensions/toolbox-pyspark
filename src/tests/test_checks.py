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

# ## Python Third Party Imports ----
import pytest
from parameterized import parameterized
from pyspark.sql import functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup, name_func_flat_list
from toolbox_pyspark.checks import (
    assert_column_exists,
    assert_columns_exists,
    assert_valid_spark_type,
    column_exists,
    columns_exists,
    is_vaid_spark_type,
    table_exists,
    warn_column_missing,
    warn_columns_missing,
)
from toolbox_pyspark.constants import VALID_PYSPARK_TYPE_NAMES
from toolbox_pyspark.io import write_to_path
from toolbox_pyspark.utils.exceptions import (
    ColumnDoesNotExistError,
    InvalidPySparkDataTypeError,
)
from toolbox_pyspark.utils.warnings import ColumnDoesNotExistWarning


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestColumnExists                                                         ####
# ---------------------------------------------------------------------------- #


class TestColumnExists(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_column_exists_1(self) -> None:
        assert column_exists(self.ps_df, "a") is True

    def test_column_exists_2(self) -> None:
        assert column_exists(self.ps_df, "c") is False

    def test_column_exists_3(self) -> None:
        assert column_exists(self.ps_df, "A", True) is False

    def test_column_exists_4(self) -> None:
        assert column_exists(self.ps_df, "a", False) is True


# ---------------------------------------------------------------------------- #
#  TestColumnsExists                                                        ####
# ---------------------------------------------------------------------------- #


class TestColumnsExists(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_columns_exists_1(self) -> None:
        assert columns_exists(self.ps_df, ["a", "b"]) is True

    def test_columns_exists_2(self) -> None:
        assert columns_exists(self.ps_df, ["b", "c"]) is False

    def test_columns_exists_3(self) -> None:
        assert (
            columns_exists(
                self.ps_df.withColumn("c", F.lit("c")).withColumn("d", F.lit("d")),
                ["b", "c", "d", "e", "f"],
            )
            is False
        )

    def test_columns_exists_4(self) -> None:
        assert columns_exists(self.ps_df, ["A", "B"], False) is True

    def test_columns_exists_6(self) -> None:
        assert columns_exists(self.ps_df, ["A", "B"], True) is False


# ---------------------------------------------------------------------------- #
#  TestAssertColumnExists                                                   ####
# ---------------------------------------------------------------------------- #


class TestAssertColumnsExists(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_assert_column_exists_1(self) -> None:
        assert assert_column_exists(self.ps_df, "a") is None

    def test_assert_column_exists_2(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_column_exists(self.ps_df, "c")

    def test_assert_column_exists_3(self) -> None:
        assert assert_column_exists(self.ps_df, "A", False) is None

    def test_assert_column_exists_4(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_column_exists(self.ps_df, "A", True)

    def test_assert_columns_exists_1(self) -> None:
        assert assert_columns_exists(self.ps_df, ["a", "b"]) is None

    def test_assert_columns_exists_2(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_columns_exists(self.ps_df, ["b", "c"])

    def test_assert_columns_exists_3(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_columns_exists(
                self.ps_df.withColumn("c", F.lit("c")).withColumn("d", F.lit("d")),
                ["b", "c", "d", "e", "f"],
            )

    def test_assert_columns_exists_4(self) -> None:
        assert assert_columns_exists(self.ps_df, ["A", "B"], False) is None

    def test_assert_columns_exists_5(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_columns_exists(self.ps_df, ["B", "C"], True)

    def test_assert_columns_exists_6(self) -> None:
        with pytest.raises(ColumnDoesNotExistError):
            assert_columns_exists(self.ps_df, ["B", "C", "D", "E"])


## --------------------------------------------------------------------------- #
##  TestWarnColumnMissing                                                   ####
## --------------------------------------------------------------------------- #


class TestWarnColumnsMissing(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_warn_column_missing_1(self) -> None:
        assert warn_column_missing(self.ps_df, "a") is None

    def test_warn_column_missing_2(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_column_missing(self.ps_df, "c")

    def test_warn_column_missing_3(self) -> None:
        assert warn_column_missing(self.ps_df, "A", False) is None

    def test_warn_column_missing_4(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_column_missing(self.ps_df, "A", True)

    def test_warn_columns_missing_1(self) -> None:
        assert warn_columns_missing(self.ps_df, ["a", "b"]) is None

    def test_warn_columns_missing_2(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_columns_missing(self.ps_df, ["b", "c"])

    def test_warn_columns_missing_3(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_columns_missing(
                self.ps_df.withColumn("c", F.lit("c")).withColumn("d", F.lit("d")),
                ["b", "c", "d", "e", "f"],
            )

    def test_warn_columns_missing_4(self) -> None:
        assert warn_columns_missing(self.ps_df, ["A", "B"], False) is None

    def test_warn_columns_missing_5(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_columns_missing(self.ps_df, ["B", "C"], True)

    def test_warn_columns_missing_6(self) -> None:
        with pytest.warns(ColumnDoesNotExistWarning):
            warn_columns_missing(self.ps_df, ["B", "C", "D", "E"])


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
            table=self.ps_df,
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
