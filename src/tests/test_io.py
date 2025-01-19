# ---------------------------------------------------------------------------- #
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
import os
import shutil
from pathlib import Path

# ## Python Third Party Imports ----
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as psDataFrame, functions as F, types as T
from toolbox_python.checkers import is_type

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.checks import assert_table_exists
from toolbox_pyspark.io import (
    read,
    read_from_path,
    read_from_table,
    transfer,
    transfer_by_path,
    transfer_by_table,
    write,
    write_to_path,
    write_to_table,
)
from toolbox_pyspark.utils.exceptions import ValidationError


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestReadingAndWriting                                                    ####
# ---------------------------------------------------------------------------- #


class TestReadingAndWriting_ByPath(PySparkSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.write_dir_name = "io"
        cls.write_path: str = f"./src/tests/{cls.write_dir_name}"
        cls.clean_up = True

    @classmethod
    def tearDownClass(cls) -> None:
        write_dir = Path(cls.write_path)
        if cls.clean_up and write_dir.exists():
            shutil.rmtree(write_dir)
        super().tearDownClass()

    def test_1_write_to_path_1(self) -> None:
        """Test writing parquet"""
        write_to_path(
            data_frame=self.ps_df,
            name="ps_df_parquet",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
            },
        )
        assert_table_exists(
            spark_session=self.spark,
            name="ps_df_parquet",
            path=self.write_path,
            data_format="parquet",
        )

    @pytest.mark.skip("`delta` package currently causing issues...")
    def test_1_write_to_path_2(self) -> None:
        """Test writing delta"""
        write_to_path(
            data_frame=self.ps_df,
            name="ps_df_delta",
            path=self.write_path,
            data_format="delta",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
            },
        )
        assert_table_exists(
            spark_session=self.spark,
            name="ps_df_delta",
            path=self.write_path,
            data_format="delta",
        )

    def test_1_write_to_path_3(self) -> None:
        """Test writing csv"""
        write_to_path(
            data_frame=self.ps_df,
            name="ps_df_csv",
            path=self.write_path,
            data_format="csv",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
                "header": "true",
            },
        )
        assert_table_exists(
            spark_session=self.spark,
            name="ps_df_csv",
            path=self.write_path,
            data_format="csv",
        )

    def test_1_write_to_path_4(self) -> None:
        """Test writing to partitioned parquet"""
        write_to_path(
            data_frame=self.ps_df,
            name="ps_df_parquet_partitioned",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            partition_cols=["a"],
        )
        assert_table_exists(
            name="ps_df_parquet_partitioned",
            path=self.write_path,
            data_format="parquet",
            spark_session=self.spark,
        )

    def test_2_read_from_path_1(self) -> None:
        """Test reading parquet"""
        table: psDataFrame = read_from_path(
            name="ps_df_parquet",
            path=self.write_path,
            spark_session=self.spark,
            data_format="parquet",
        )
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)

    @pytest.mark.skip("`delta` package currently causing issues...")
    def test_2_read_from_path_2(self) -> None:
        """Test reading delta"""
        table: psDataFrame = read_from_path(
            name="ps_df_delta",
            path=self.write_path,
            spark_session=self.spark,
            data_format="delta",
        )
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)

    def test_2_read_from_path_3(self) -> None:
        """Test reading csv"""
        result: psDataFrame = read_from_path(
            name="ps_df_csv",
            path=self.write_path,
            spark_session=self.spark,
            data_format="csv",
            read_options={"header": "true"},
        )
        result = result.withColumn("a", F.col("a").cast("long"))
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)

    def test_2_read_from_path_4(self) -> None:
        """Test reading from partitioned parquet"""
        name = "ps_df_parquet_partitioned"
        table: psDataFrame = (
            read_from_path(
                name=name,
                path=self.write_path,
                spark_session=self.spark,
                data_format="parquet",
            )
            .select(self.ps_df.columns)
            .withColumn("a", F.col("a").cast("long"))
            .orderBy("a")
        )
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)
        partitions: list[str] = os.listdir(f"{self.write_path}/{name}")
        assert len([obj for obj in partitions if "=" in obj]) > 0

    def test_3_transfer_table_1(self) -> None:

        # Write new table
        write_to_path(
            data_frame=self.ps_df_extended,
            name="ps_df_extended",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )

        # Transfer new table
        transfer_by_path(
            spark_session=self.spark,
            from_table_path=self.write_path,
            from_table_name="ps_df_extended",
            from_table_format="parquet",
            to_table_path=self.write_path,
            to_table_name="ps_df_extended_transferred",
            to_table_format="parquet",
            to_table_mode="overwrite",
        )

        # Read transferred table
        table: psDataFrame = read_from_path(
            name="ps_df_extended_transferred",
            path=self.write_path,
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_3_transfer_table_2(self) -> None:

        # Transfer new table
        transfer_by_path(
            spark_session=self.spark,
            from_table_path=self.write_path,
            from_table_name="ps_df_extended",
            from_table_format="parquet",
            to_table_path=self.write_path,
            to_table_name="ps_df_extended_transferred2",
            to_table_format="parquet",
            to_table_mode="overwrite",
            to_table_partition_cols=["a", "b"],
        )

        # Read transferred table
        table: psDataFrame = read_from_path(
            name="ps_df_extended_transferred2",
            path=self.write_path,
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table.withColumn("a", F.col("a").cast(T.LongType()))
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True, ignore_column_order=True)
        partitions: list[str] = os.listdir(f"{self.write_path}/ps_df_extended_transferred2")
        assert len([obj for obj in partitions if "=" in obj]) > 0

    def test_3_transfer_table_3(self) -> None:
        """Transfer using generic function"""

        # Write new table
        write(
            data_frame=self.ps_df_extended,
            name="ps_df_extended2",
            path=self.write_path,
            method="path",
            data_format="parquet",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
            },
        )

        # Transfer new table
        transfer(
            spark_session=self.spark,
            method="path",
            from_table_path=self.write_path,
            from_table_name="ps_df_extended2",
            from_table_format="parquet",
            to_table_path=self.write_path,
            to_table_name="ps_df_extended_transferred3",
            to_table_format="parquet",
            to_table_mode="overwrite",
        )

        # Read transferred table
        table: psDataFrame = read(
            name="ps_df_extended_transferred3",
            path=self.write_path,
            method="path",
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)


class TestReadingAndWriting_ByTable(PySparkSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.write_dir_name = "io"
        cls.schema = "testing"
        cls.write_path = str(Path(f"./src/tests/{cls.write_dir_name}").absolute())
        cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cls.schema} LOCATION '{cls.write_path}'")
        cls.spark.catalog.setCurrentDatabase(cls.schema)
        cls.spark.catalog.currentDatabase()
        cls.clean_up = True

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.clean_up:
            for tbl in cls.spark.catalog.listTables():
                cls.spark.sql(f"DROP TABLE IF EXISTS {cls.schema}.{tbl.name}")
            cls.spark.sql(f"DROP SCHEMA IF EXISTS {cls.schema}")
        super().tearDownClass()

    def test_1_write_to_table_1(self) -> None:
        """Test writing table"""
        write_to_table(
            data_frame=self.ps_df,
            name="ps_df_table",
            schema=self.schema,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )
        assert "ps_df_table" in [
            tbl.name for tbl in self.spark.catalog.listTables(self.schema)
        ]

    def test_1_write_to_table_2(self) -> None:
        """Test writing to partitioned table"""
        write_to_table(
            data_frame=self.ps_df,
            name="ps_df_table_partitioned",
            schema=self.schema,
            data_format="parquet",
            mode="overwrite",
            partition_cols=["a"],
        )
        assert "ps_df_table_partitioned" in [
            tbl.name for tbl in self.spark.catalog.listTables(self.schema)
        ]

    def test_2_read_from_table_1(self) -> None:
        """Test reading table"""
        table: psDataFrame = read_from_table(
            name="ps_df_table",
            schema=self.schema,
            spark_session=self.spark,
            data_format="parquet",
        )
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected, ignore_column_order=True)

    def test_2_read_from_table_2(self) -> None:
        """Test reading from partitioned table"""
        table: psDataFrame = (
            read_from_table(
                name="ps_df_table_partitioned",
                schema=self.schema,
                spark_session=self.spark,
                data_format="parquet",
            )
            .select(self.ps_df.columns)
            .withColumn("a", F.col("a").cast("long"))
            .orderBy("a")
        )
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)

    def test_3_transfer_table_1(self) -> None:
        """Test transferring table"""

        # Write new table
        write_to_table(
            data_frame=self.ps_df_extended,
            name="ps_df_extended",
            schema=self.schema,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )

        # Transfer new table
        transfer_by_table(
            spark_session=self.spark,
            from_table_name="ps_df_extended",
            from_table_schema=self.schema,
            from_table_format="parquet",
            from_table_options={"path": self.write_path},
            to_table_name="ps_df_extended_transferred",
            to_table_schema=self.schema,
            to_table_format="parquet",
            to_table_mode="overwrite",
            to_table_options={"overwriteSchema": "true"},
        )

        # Read transferred table
        table: psDataFrame = read_from_table(
            name="ps_df_extended_transferred",
            schema=self.schema,
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_3_transfer_table_2(self) -> None:
        """Test transferring partitioned table"""

        # Transfer new table
        transfer_by_table(
            spark_session=self.spark,
            from_table_name="ps_df_extended",
            from_table_schema=self.schema,
            from_table_format="parquet",
            to_table_name="ps_df_extended_transferred2",
            to_table_schema=self.schema,
            to_table_format="parquet",
            to_table_mode="overwrite",
            to_table_partition_cols=["a", "b"],
            to_table_options={"overwriteSchema": "true"},
        )

        # Read transferred table
        table: psDataFrame = read_from_table(
            name="ps_df_extended_transferred2",
            schema=self.schema,
            spark_session=self.spark,
            data_format="parquet",
            read_options={"path": self.write_path},
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(
            result,
            expected,
            ignore_nullable=True,
            ignore_column_order=True,
            ignore_row_order=True,
        )
        print(self.spark.catalog.recoverPartitions("ps_df_extended_transferred2"))
        partitions: list[str] = os.listdir(f"{self.write_path}/ps_df_extended_transferred2")
        assert len([obj for obj in partitions if "=" in obj]) > 0

    def test_3_transfer_table_3(self) -> None:
        """Transfer using generic function"""

        # Write new table
        write(
            data_frame=self.ps_df_extended,
            name="ps_df_extended",
            method="table",
            schema=self.schema,
            data_format="parquet",
            mode="overwrite",
            write_options={"overwriteSchema": "true"},
        )

        # Transfer new table
        transfer(
            spark_session=self.spark,
            method="table",
            from_table_name="ps_df_extended",
            from_table_schema=self.schema,
            from_table_format="parquet",
            from_table_options={"path": self.write_path},
            to_table_name="ps_df_extended_transferred",
            to_table_schema=self.schema,
            to_table_format="parquet",
            to_table_mode="overwrite",
            to_table_options={"overwriteSchema": "true"},
        )

        # Read transferred table
        table: psDataFrame = read(
            name="ps_df_extended_transferred",
            method="table",
            schema=self.schema,
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert is_type(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_errors(self) -> None:
        with pytest.raises(ValidationError):
            read_from_table(spark_session=self.spark, name="schema/table")
        with pytest.raises(ValidationError):
            read_from_table(spark_session=self.spark, name="source.schema.table")
