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
from typing import LiteralString

# ## Python Third Party Imports ----
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame as psDataFrame, functions as F

# ## Local First Party Imports ----
from tests.setup import PySparkSetup
from toolbox_pyspark.io import read_from_path, transfer_table, write_to_path


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Test Suite                                                            ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  TestReadingAndWriting                                                    ####
# ---------------------------------------------------------------------------- #


class TestReadingAndWriting(PySparkSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.write_dir_name = "io"
        cls.write_path: LiteralString = f"./src/tests/{cls.write_dir_name}"
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
            table=self.ps_df,
            name="ps_df_parquet",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
            },
        )

    @pytest.mark.skip("`delta` package currently causing issues...")
    def test_1_write_to_path_2(self) -> None:
        """Test writing delta"""
        write_to_path(
            table=self.ps_df,
            name="ps_df_delta",
            path=self.write_path,
            data_format="delta",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
            },
        )

    def test_1_write_to_path_3(self) -> None:
        """Test writing csv"""
        write_to_path(
            table=self.ps_df,
            name="ps_df_csv",
            path=self.write_path,
            data_format="csv",
            mode="overwrite",
            write_options={
                "overwriteSchema": "true",
                "header": "true",
            },
        )

    def test_1_write_to_path_4(self) -> None:
        """Test writing to partitioned parquet"""
        write_to_path(
            table=self.ps_df,
            name="ps_df_parquet_partitioned",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            partition_cols=["a"],
        )

    def test_2_read_from_path_1(self) -> None:
        """Test reading parquet"""
        table: psDataFrame = read_from_path(
            name="ps_df_parquet",
            path=self.write_path,
            spark_session=self.spark,
            data_format="parquet",
        )
        assert isinstance(table, psDataFrame)
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
        assert isinstance(table, psDataFrame)
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
        ).orderBy("a")
        assert isinstance(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df
        assert_df_equality(result, expected)
        partitions: list[str] = os.listdir(f"{self.write_path}/{name}")
        assert len([obj for obj in partitions if "=" in obj]) > 0

    def test_3_transfer_table_1(self) -> None:

        # Write new table
        write_to_path(
            table=self.ps_df_extended,
            name="ps_df_extended",
            path=self.write_path,
            data_format="parquet",
            mode="overwrite",
            write_options={
                "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
                "overwriteSchema": "true",
            },
        )

        # Transfer new table
        transfer_table(
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
        assert isinstance(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)

    def test_3_transfer_table_2(self) -> None:

        # Transfer new table
        transfer_table(
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
            name="ps_df_extended_transferred",
            path=self.write_path,
            spark_session=self.spark,
            data_format="parquet",
        )

        # Test
        assert isinstance(table, psDataFrame)
        result: psDataFrame = table
        expected: psDataFrame = self.ps_df_extended
        assert_df_equality(result, expected, ignore_nullable=True)
        partitions: list[str] = os.listdir(
            f"{self.write_path}/ps_df_extended_transferred2"
        )
        assert len([obj for obj in partitions if "=" in obj]) > 0
