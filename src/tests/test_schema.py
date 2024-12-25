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
from pprint import pformat

# ## Python Third Party Imports ----
import pytest
from parameterized import parameterized
from pyspark.sql import types as T

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.cleaning import update_nullability
from toolbox_pyspark.io import write_to_path
from toolbox_pyspark.schema import check_schemas_match, view_schema_differences


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestSchemas                                                              ####
# ---------------------------------------------------------------------------- #


class TestCheckSchemas(PySparkSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.write_dir_name = "io"
        cls.write_path = f"./src/tests/{cls.write_dir_name}"
        cls.clean_up = True

    @classmethod
    def tearDownClass(cls) -> None:
        write_dir = Path(cls.write_path)
        if cls.clean_up and write_dir.exists():
            shutil.rmtree(write_dir)
        super().tearDownClass()

    def setUp(self) -> None:
        write_to_path(
            table=self.ps_df_schema_left,
            name="left",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )
        write_to_path(
            table=self.ps_df_schema_right,
            name="right",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )
        self.expected = [
            (
                "add",
                {"left": T.StructField("e", T.StringType(), False)},
            ),
            (
                "remove",
                {"right": T.StructField("g", T.StringType(), False)},
            ),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), False),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
            (
                "change_nullable",
                {
                    "left": T.StructField("c", T.StringType(), False),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]

    @parameterized.expand(
        input=[
            "table",
            "table_table",
            "tables",
            "by_table",
            "by_table_and_table",
            "table_and_table",
        ],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join(params[0])}",
    )
    def test_check_schemas_match_params(self, method) -> None:
        result = check_schemas_match(
            method=method,
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            return_object="results",
        )
        assert result == self.expected

    def test_check_schemas_match_1(self) -> None:
        """table_table"""
        result = check_schemas_match(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            return_object="results",
        )
        assert result == self.expected

    def test_check_schemas_match_2(self) -> None:
        """table_path"""
        result = check_schemas_match(
            method="table_path",
            spark_session=self.spark,
            left_table=self.ps_df_schema_left.transform(update_nullability, nullable=True),
            right_table_path=self.write_path,
            right_table_name="right",
            right_table_format="parquet",
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            return_object="results",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_check_schemas_match_3(self) -> None:
        """path_table"""
        result = check_schemas_match(
            method="path_table",
            spark_session=self.spark,
            left_table_path=self.write_path,
            left_table_name="left",
            left_table_format="parquet",
            right_table=self.ps_df_schema_right.transform(update_nullability, nullable=True),
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            return_object="results",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_check_schemas_match_4(self) -> None:
        """path_path"""
        result = check_schemas_match(
            method="path_path",
            spark_session=self.spark,
            left_table_path=self.write_path,
            left_table_name="left",
            left_table_format="parquet",
            right_table_path=self.write_path,
            right_table_name="right",
            right_table_format="parquet",
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            return_object="results",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_check_schemas_match_5(self) -> None:
        """error"""
        with pytest.raises(AttributeError):
            _ = check_schemas_match(method="error")

    def test_check_schemas_match_6(self) -> None:
        """check same"""
        result = check_schemas_match(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_left,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            return_object="check",
        )
        assert result is True

    def test_check_schemas_match_7(self) -> None:
        """check different"""
        result = check_schemas_match(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            return_object="check",
        )
        assert result is False


class TestViewSchemas(PySparkSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.write_dir_name = "io"
        cls.write_path = f"./src/tests/{cls.write_dir_name}"
        cls.clean_up = True

    @classmethod
    def tearDownClass(cls) -> None:
        write_dir = Path(cls.write_path)
        if cls.clean_up and write_dir.exists():
            shutil.rmtree(write_dir)
        super().tearDownClass()

    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys) -> None:
        self.capsys: pytest.CaptureFixture = capsys

    def setUp(self) -> None:
        write_to_path(
            table=self.ps_df_schema_left,
            name="left",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )
        write_to_path(
            table=self.ps_df_schema_right,
            name="right",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )
        self.expected = [
            ("add", {"left": T.StructField("e", T.StringType(), False)}),
            ("remove", {"right": T.StructField("g", T.StringType(), False)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), False),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
            (
                "change_nullable",
                {
                    "left": T.StructField("c", T.StringType(), False),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]

    @parameterized.expand(
        input=[
            "table",
            "table_table",
            "tables",
            "by_table",
            "by_table_and_table",
            "table_and_table",
        ],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join(params[0])}",
    )
    def test_view_schema_differences_params(self, method) -> None:
        result = view_schema_differences(
            method=method,
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            view_type="return",
        )
        assert result == self.expected

    def test_view_schema_differences_1(self) -> None:
        """table_table"""
        result = view_schema_differences(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            view_type="return",
        )
        assert result == self.expected

    def test_view_schema_differences_2(self) -> None:
        """table_path"""
        result = view_schema_differences(
            method="table_path",
            spark_session=self.spark,
            left_table=self.ps_df_schema_left.transform(update_nullability, nullable=True),
            right_table_path=self.write_path,
            right_table_name="right",
            right_table_format="parquet",
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            view_type="return",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_view_schema_differences_3(self) -> None:
        """path_table"""
        result = view_schema_differences(
            method="path_table",
            spark_session=self.spark,
            left_table_path=self.write_path,
            left_table_name="left",
            left_table_format="parquet",
            right_table=self.ps_df_schema_right.transform(update_nullability, nullable=True),
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            view_type="return",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_view_schema_differences_4(self) -> None:
        """path_path"""
        result = view_schema_differences(
            method="path_path",
            spark_session=self.spark,
            left_table_path=self.write_path,
            left_table_name="left",
            left_table_format="parquet",
            right_table_path=self.write_path,
            right_table_name="right",
            right_table_format="parquet",
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=False,
            view_type="return",
        )
        expected = [
            ("add", {"left": T.StructField("e", T.StringType(), True)}),
            ("remove", {"right": T.StructField("g", T.StringType(), True)}),
            (
                "change_type",
                {
                    "left": T.StructField("c", T.StringType(), True),
                    "right": T.StructField("c", T.IntegerType(), True),
                },
            ),
        ]
        assert result == expected

    def test_view_schema_differences_5(self) -> None:
        """print"""
        result = view_schema_differences(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            view_type="print",
        )
        captured = self.capsys.readouterr().out
        assert result is None
        assert captured.strip() == str(self.expected).strip()

    def test_view_schema_differences_6(self) -> None:
        """pprint"""
        result = view_schema_differences(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_right,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            view_type="pprint",
        )
        captured = self.capsys.readouterr().out
        assert result is None
        assert captured.strip() == pformat(self.expected).strip()

    def test_view_schema_differences_7(self) -> None:
        """same"""
        result = view_schema_differences(
            method="table_table",
            left_table=self.ps_df_schema_left,
            right_table=self.ps_df_schema_left,
            include_add_field=True,
            include_change_field=True,
            include_remove_field=True,
            include_change_nullable=True,
            view_type="print",
        )
        assert result is None

    def test_view_schema_differences_8(self) -> None:
        """error"""
        with pytest.raises(AttributeError):
            _ = view_schema_differences(method="error")
