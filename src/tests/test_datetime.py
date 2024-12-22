# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
import pytest
from chispa.dataframe_comparer import assert_df_equality
from parameterized import parameterized
from pyspark.sql import functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.datetime import (
    add_local_datetime_column,
    add_local_datetime_columns,
    rename_datetime_column,
    rename_datetime_columns,
    split_datetime_column,
    split_datetime_columns,
)
from toolbox_pyspark.utils.exceptions import ColumnDoesNotExistError


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestDateTimeColumnNames                                                  ####
# ---------------------------------------------------------------------------- #


class TestDateTimeColumnNames(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_rename_datetime_columns_1(self) -> None:
        "single"
        result = rename_datetime_column(self.ps_df_timestamp, "c_date").columns
        expected = ["a", "b", "c_dateTIME", "d_date"]
        assert result == expected

    def test_rename_datetime_columns_2(self) -> None:
        "single error"
        with pytest.raises(ColumnDoesNotExistError):
            _ = rename_datetime_column(self.ps_df_timestamp, "test")

    def test_rename_datetime_columns_3(self) -> None:
        "default"
        result = rename_datetime_columns(self.ps_df_timestamp).columns
        expected = ["a", "b", "c_dateTIME", "d_dateTIME"]
        assert result == expected

    def test_rename_datetime_columns_4(self) -> None:
        "single column"
        result = rename_datetime_columns(
            dataframe=self.ps_df_timestamp, columns="c_date"
        ).columns
        expected = ["a", "b", "c_dateTIME", "d_date"]
        assert result == expected

    def test_rename_datetime_columns_5(self) -> None:
        "no change"
        result = rename_datetime_columns(
            dataframe=self.ps_df_timestamp.withColumn(
                "c_dateTIME", F.lit(F.current_timestamp()).cast("timestamp")
            ),
            columns=["c_date"],
        ).columns
        expected = ["a", "b", "c_date", "d_date", "c_dateTIME"]
        assert result == expected

    def test_rename_datetime_columns_6(self) -> None:
        "defined list"
        result = rename_datetime_columns(
            dataframe=self.ps_df_timestamp, columns=["c_date"]
        ).columns
        expected = ["a", "b", "c_dateTIME", "d_date"]
        assert result == expected

    def test_rename_datetime_columns_7(self) -> None:
        "defined list with error"
        with pytest.raises(ColumnDoesNotExistError):
            _ = rename_datetime_columns(
                dataframe=self.ps_df_timestamp, columns=["c_date", "test"]
            )

    def test_rename_datetime_columns_8(self) -> None:
        "parsed 'all'"
        result = rename_datetime_columns(self.ps_df_timestamp, columns="all").columns
        expected = ["a", "b", "c_dateTIME", "d_dateTIME"]
        assert result == expected


# ---------------------------------------------------------------------------- #
#  TestLocalDateTimeColumns                                                 ####
# ---------------------------------------------------------------------------- #


class TestLocalDateTimeColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_add_local_datetime_columns_1(self) -> None:
        """Basic check"""
        result = self.ps_df_timezone.columns
        expected = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "target",
            "TIMEZONE_LOCATION",
        ]
        assert result == expected

    def test_add_local_datetime_columns_2(self) -> None:
        """Single column, default config"""
        result = add_local_datetime_column(
            dataframe=self.ps_df_timezone,
            column="c",
        )
        expected = self.ps_df_timezone.withColumn(
            "c_LOCAL",
            F.from_utc_timestamp(
                F.col("c").cast("timestamp"),
                F.col("TIMEZONE_LOCATION"),
            ),
        )
        assert_df_equality(result, expected)

    def test_add_local_datetime_columns_3(self) -> None:
        """Single column, custom config"""
        result = add_local_datetime_column(
            dataframe=self.ps_df_timezone,
            column="c",
            from_timezone="Australia/Sydney",
            column_with_target_timezone="target",
        )
        expected = self.ps_df_timezone.withColumn(
            "c_UTC",
            F.to_utc_timestamp(
                F.col("c").cast("timestamp"),
                "Australia/Sydney",
            ),
        ).withColumn(
            "c_LOCAL",
            F.from_utc_timestamp(
                F.col("c_UTC").cast("timestamp"),
                F.col("target"),
            ),
        )
        assert_df_equality(result, expected)

    def test_add_local_datetime_columns_4(self) -> None:
        """Multiple columns, default config"""
        result = add_local_datetime_columns(dataframe=self.ps_df_timezone_extended)
        expected = self.ps_df_timezone_extended.withColumn(
            "d_datetime_LOCAL",
            F.from_utc_timestamp(
                F.col("d_datetime").cast("timestamp"),
                F.col("TIMEZONE_LOCATION"),
            ),
        ).withColumn(
            "e_datetime_LOCAL",
            F.from_utc_timestamp(
                F.col("e_datetime").cast("timestamp"),
                F.col("TIMEZONE_LOCATION"),
            ),
        )
        assert_df_equality(result, expected)

    def test_add_local_datetime_columns_5(self) -> None:
        """Multiple columns, semi-custom config"""
        result = add_local_datetime_columns(
            dataframe=self.ps_df_timezone_extended,
            columns=["c", "d_datetime"],
        )
        expected = self.ps_df_timezone_extended.withColumn(
            "c_LOCAL",
            F.from_utc_timestamp(
                F.col("c").cast("timestamp"),
                F.col("TIMEZONE_LOCATION"),
            ),
        ).withColumn(
            "d_datetime_LOCAL",
            F.from_utc_timestamp(
                F.col("d_datetime").cast("timestamp"),
                F.col("TIMEZONE_LOCATION"),
            ),
        )
        assert_df_equality(result, expected)

    def test_add_local_datetime_columns_6(self) -> None:
        """Multiple columns, full-custom config"""
        result = add_local_datetime_columns(
            dataframe=self.ps_df_timezone_extended,
            columns=["c", "d_datetime"],
            from_timezone="Australia/Sydney",
            column_with_target_timezone="target",
        )
        expected = (
            self.ps_df_timezone_extended.withColumn(
                "c_UTC",
                F.to_utc_timestamp(
                    F.col("c").cast("timestamp"),
                    "Australia/Sydney",
                ),
            )
            .withColumn(
                "c_LOCAL",
                F.from_utc_timestamp(
                    F.col("c_UTC").cast("timestamp"),
                    F.col("target"),
                ),
            )
            .withColumn(
                "d_datetime_UTC",
                F.to_utc_timestamp(
                    F.col("d_datetime").cast("timestamp"),
                    "Australia/Sydney",
                ),
            )
            .withColumn(
                "d_datetime_LOCAL",
                F.from_utc_timestamp(
                    F.col("d_datetime_UTC").cast("timestamp"),
                    F.col("target"),
                ),
            )
        )
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=[None, "all"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(prm) for prm in params[0]])}",
    )
    def test_add_local_datetime_columns_7(self, columns) -> None:
        result = add_local_datetime_columns(
            dataframe=self.ps_df_timezone_extended,
            columns=columns,
            from_timezone="Australia/Sydney",
            column_with_target_timezone="target",
        )
        expected = (
            self.ps_df_timezone_extended.withColumn(
                "d_datetime_UTC",
                F.to_utc_timestamp(
                    F.col("d_datetime").cast("timestamp"),
                    "Australia/Sydney",
                ),
            )
            .withColumn(
                "d_datetime_LOCAL",
                F.from_utc_timestamp(
                    F.col("d_datetime_UTC").cast("timestamp"),
                    F.col("target"),
                ),
            )
            .withColumn(
                "e_datetime_UTC",
                F.to_utc_timestamp(
                    F.col("e_datetime").cast("timestamp"),
                    "Australia/Sydney",
                ),
            )
            .withColumn(
                "e_datetime_LOCAL",
                F.from_utc_timestamp(
                    F.col("e_datetime_UTC").cast("timestamp"),
                    F.col("target"),
                ),
            )
        )
        assert_df_equality(result, expected)

    def test_add_local_datetime_columns_8(self) -> None:
        "single column"
        result = add_local_datetime_columns(
            dataframe=self.ps_df_timezone_extended,
            columns="c",
            from_timezone="Australia/Sydney",
            column_with_target_timezone="target",
        )
        expected = self.ps_df_timezone_extended.withColumn(
            "c_UTC",
            F.to_utc_timestamp(
                F.col("c").cast("timestamp"),
                "Australia/Sydney",
            ),
        ).withColumn(
            "c_LOCAL",
            F.from_utc_timestamp(
                F.col("c_UTC").cast("timestamp"),
                F.col("target"),
            ),
        )
        assert_df_equality(result, expected)


# ---------------------------------------------------------------------------- #
#  TestSplitDateTimeColumns                                                 ####
# ---------------------------------------------------------------------------- #


class TestSplitDateTimeColumns(PySparkSetup):
    def setUp(self) -> None:
        pass

    def test_split_datetime_columns_1(self) -> None:
        """Basic check"""
        result = self.ps_df_datetime.columns
        expected = [
            "a",
            "b",
            "c_datetime",
            "d_datetime",
            "e_datetime",
            "TIMEZONE_LOCATION",
        ]
        assert result == expected

    def test_split_datetime_columns_2(self) -> None:
        """Single column, default config"""
        result = split_datetime_column(self.ps_df_datetime, "c_datetime")
        expected = self.ps_df_datetime.withColumns(
            {
                "C_DATE": F.expr(
                    f"cast(date_format(c_datetime, 'yyyy-MM-dd') as date)"
                ),
                "C_TIME": F.expr(
                    f"cast(date_format(c_datetime, 'HH:mm:ss') as string)"
                ),
            }
        )
        assert_df_equality(result, expected)

    def test_split_datetime_columns_3(self) -> None:
        """Multiple columns, default config"""
        result = split_datetime_columns(self.ps_df_datetime)
        cols_exprs = {}
        for column in self.ps_df_datetime.columns:
            if not "datetime" in column.lower():
                continue
            col_date_name = f"{column.upper().replace('DATETIME','DATE')}"
            col_time_name = f"{column.upper().replace('DATETIME','TIME')}"
            col_date_value = F.expr(
                f"cast(date_format({column}, 'yyyy-MM-dd') as date)"
            )
            col_time_value = F.expr(
                f"cast(date_format({column}, 'HH:mm:ss') as string)"
            )
            cols_exprs[col_date_name] = col_date_value
            cols_exprs[col_time_name] = col_time_value
        expected = self.ps_df_datetime.withColumns(cols_exprs)
        assert_df_equality(result, expected)

    def test_split_datetime_columns_4(self) -> None:
        """Multiple columns, custom config"""
        result = split_datetime_columns(
            self.ps_df_datetime,
            ["c_datetime", "d_datetime"],
        )
        cols_exprs = {}
        for column in ["c_datetime", "d_datetime"]:
            col_date_name = f"{column.upper().replace('DATETIME','DATE')}"
            col_time_name = f"{column.upper().replace('DATETIME','TIME')}"
            col_date_value = F.expr(
                f"cast(date_format({column}, 'yyyy-MM-dd') as date)"
            )
            col_time_value = F.expr(
                f"cast(date_format({column}, 'HH:mm:ss') as string)"
            )
            cols_exprs[col_date_name] = col_date_value
            cols_exprs[col_time_name] = col_time_value
        expected = self.ps_df_datetime.withColumns(cols_exprs)
        assert_df_equality(result, expected)

    @parameterized.expand(
        input=[None, "all"],
        name_func=lambda func, idx, params: f"{func.__name__}_{idx}_{'_'.join([str(prm) for prm in params[0]])}",
    )
    def test_split_datetime_columns_5(self, columns) -> None:
        """Multiple columns, default config"""
        result = split_datetime_columns(
            self.ps_df_datetime,
            columns=columns,
        )
        cols_exprs = {}
        for column in self.ps_df_datetime.columns:
            if not "datetime" in column.lower():
                continue
            col_date_name = f"{column.upper().replace('DATETIME','DATE')}"
            col_time_name = f"{column.upper().replace('DATETIME','TIME')}"
            col_date_value = F.expr(
                f"cast(date_format({column}, 'yyyy-MM-dd') as date)"
            )
            col_time_value = F.expr(
                f"cast(date_format({column}, 'HH:mm:ss') as string)"
            )
            cols_exprs[col_date_name] = col_date_value
            cols_exprs[col_time_name] = col_time_value
        expected = self.ps_df_datetime.withColumns(cols_exprs)
        assert_df_equality(result, expected)

    def test_split_datetime_columns_6(self) -> None:
        """Single column"""
        result = split_datetime_columns(self.ps_df_datetime, "c_datetime")
        expected = self.ps_df_datetime.withColumns(
            {
                "C_DATE": F.expr(
                    f"cast(date_format(c_datetime, 'yyyy-MM-dd') as date)"
                ),
                "C_TIME": F.expr(
                    f"cast(date_format(c_datetime, 'HH:mm:ss') as string)"
                ),
            }
        )
        assert_df_equality(result, expected)
