# Modules


## Overview

There are 12 modules used in this package, which covers 41 functions


### Module Descriptions

| Module                        | Description |
|-------------------------------|-------------|
| [io](./io.md)                 | The `io` module is used for reading and writing tables to/from directories.
| [checks](./checks.md)         | The `checks` module is used to check and validate various attributed about a given `pyspark` dataframe.
| [types](./types.md)           | The `types` module is used to get, check, and change a datafames column data types.
<!--
| [cleaning](./cleaning.md)     | The `cleaning` module is used to clean, fix, and fetch various aspects on a given DataFrame.
| [columns](./columns.md)       | The `columns` module is used to fetch columns from a given DataFrame using convenient syntax.
| [constants](./constants.md)   | The `constants` module is used to hold the definitions of all constant values used across the package.
| [datetime](./datetime.md)     | The `datetime` module is used for fixing column names that contain datetime data, adding conversions to local datetimes, and for splitting a column in to their date and time components.
| [delta](./delta.md)           | The `delta` module is for various processes related to Delta Lake tables. Including optimising tables, merging tables, retrieving table history, and transferring between locations.
| [dimensions](./dimensions.md) | The `dimensions` module is used for checking the dimensions of `pyspark` `dataframe`'s.
| [scale](./scale.md)           | The `scale` module is used for rounding a column (or columns) to a given rounding accuracy.
| [schema](./schema.md)         | The `schema` module is used for checking, validating, and viewing any schema differences between two different tables, either from in-memory variables, or pointing to locations on disk.
-->


### Functions by Module

| Module                                     | Function |
|--------------------------------------------|----------|
| [`io`][toolbox_pyspark.io]                 | [`read_from_path()`][toolbox_pyspark.io.read_from_path] |
|                                            | [`write_to_path()`][toolbox_pyspark.io.write_to_path] |
|                                            | [`transfer_table()`][toolbox_pyspark.io.transfer_table] |
|                                            | |
| [`checks`][toolbox_pyspark.checks]         | [`column_exists()`][toolbox_pyspark.checks.column_exists] |
|                                            | [`columns_exists()`][toolbox_pyspark.checks.columns_exists] |
|                                            | [`is_vaid_spark_type()`][toolbox_pyspark.checks.is_vaid_spark_type] |
|                                            | [`table_exists()`][toolbox_pyspark.checks.table_exists] |
|                                            | |
| [`types`][toolbox_pyspark.types]           | [`get_column_types()`][toolbox_pyspark.types.get_column_types] |
|                                            | [`cast_column_to_type()`][toolbox_pyspark.types.cast_column_to_type] |
|                                            | [`cast_columns_to_type()`][toolbox_pyspark.types.cast_columns_to_type] |
|                                            | [`map_cast_columns_to_type()`][toolbox_pyspark.types.map_cast_columns_to_type] |
<!--
| [`cleaning`][toolbox_pyspark.cleaning]     | [`create_empty_dataframe()`][toolbox_pyspark.cleaning.create_empty_dataframe] |
|                                            | [`keep_first_record_by_columns()`][toolbox_pyspark.cleaning.keep_first_record_by_columns] |
|                                            | [`convert_dataframe()`][toolbox_pyspark.cleaning.convert_dataframe] |
|                                            | [`get_column_values()`][toolbox_pyspark.cleaning.get_column_values] |
|                                            | [`update_nullability()`][toolbox_pyspark.cleaning.update_nullability] |
|                                            | [`trim_spaces_from_column()`][toolbox_pyspark.cleaning.trim_spaces_from_column] |
|                                            | [`trim_spaces_from_columns()`][toolbox_pyspark.cleaning.trim_spaces_from_columns] |
|                                            | [`apply_function_to_column()`][toolbox_pyspark.cleaning.apply_function_to_column] |
|                                            | [`apply_function_to_columns()`][toolbox_pyspark.cleaning.apply_function_to_columns] |
|                                            | [`drop_matching_rows()`][toolbox_pyspark.cleaning.drop_matching_rows] |
|                                            | |
| [`columns`][toolbox_pyspark.columns]       | [`get_columns()`][toolbox_pyspark.cleaning.get_columns] |
|                                            | [`get_columns_by_likeness()`][toolbox_pyspark.columns.get_columns_by_likeness] |
|                                            | [`rename_columns()`][toolbox_pyspark.columns.rename_columns] |
|                                            | [`reorder_columns()`][toolbox_pyspark.columns.reorder_columns] |
|                                            | [`delete_columns()`][toolbox_pyspark.columns.delete_columns] |
|                                            | |
| [`constants`][toolbox_pyspark.constants]   | |
|                                            | |
| [`datetime`][toolbox_pyspark.datetime]     | [`rename_datetime_columns()`][toolbox_pyspark.datetime.rename_datetime_columns] |
|                                            | [`rename_datetime_column()`][toolbox_pyspark.datetime.rename_datetime_column] |
|                                            | [`add_local_datetime_columns()`][toolbox_pyspark.datetime.add_local_datetime_columns] |
|                                            | [`add_local_datetime_column()`][toolbox_pyspark.datetime.add_local_datetime_column] |
|                                            | [`split_datetime_column()`][toolbox_pyspark.datetime.split_datetime_column] |
|                                            | [`split_datetime_columns()`][toolbox_pyspark.datetime.split_datetime_columns] |
|                                            | |
| [`delta`][toolbox_pyspark.delta]           | [`load_table()`][toolbox_pyspark.delta.load_table] |
|                                            | [`count_rows()`][toolbox_pyspark.delta.count_rows] |
|                                            | [`get_history()`][toolbox_pyspark.delta.get_history] |
|                                            | [`optimise_table()`][toolbox_pyspark.delta.optimise_table] |
|                                            | [`retry_optimise_table()`][toolbox_pyspark.delta.retry_optimise_table] |
|                                            | [`merge_spark_to_delta()`][toolbox_pyspark.delta.merge_spark_to_delta] |
|                                            | [`merge_delta_to_delta()`][toolbox_pyspark.delta.merge_delta_to_delta] |
|                                            | [`retry_merge_spark_to_delta()`][toolbox_pyspark.delta.retry_merge_spark_to_delta] |
|                                            | [`DeltaLoader()`][toolbox_pyspark.delta.DeltaLoader] |
|                                            | |
| [`dimensions`][toolbox_pyspark.dimensions] | [`get_dims()`][toolbox_pyspark.dimensions.get_dims] |
|                                            | [`get_dims_of_tables()`][toolbox_pyspark.dimensions.get_dims_of_tables] |
|                                            | |
| [`keys`][toolbox_pyspark.keys]             | [`add_keys_from_columns()`][toolbox_pyspark.keys.add_keys_from_columns] |
|                                            | [`add_key_from_columns()`][toolbox_pyspark.keys.add_key_from_columns] |
|                                            | |
| [`scale`][toolbox_pyspark.scale]           | [`round_column()`][toolbox_pyspark.scale.round_column] |
|                                            | [`round_columns()`][toolbox_pyspark.scale.round_columns] |
|                                            | |
| [`schema`][toolbox_pyspark.schema]         | [`view_schema_differences()`][toolbox_pyspark.schema.view_schema_differences] |
|                                            | [`check_schemas_match()`][toolbox_pyspark.schema.check_schemas_match] |
|                                            | |
-->


## Testing

This package is fully tested against:

1. Unit tests
1. Lint tests
1. MyPy tests
1. Build tests


### Latest Code Coverage

<div style="position:relative; border:none; width:100%; height:100%; display:block; overflow:auto;">
    <iframe src="../../assets/coverage/index.html" style="width:100%; height:800px;"></iframe>
</div>
