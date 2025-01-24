::: toolbox_pyspark.checks
    options:
        heading_level: 2
        members:
            -


## Column Existence

::: toolbox_pyspark.checks
    options:
        show_root_heading: false
        heading_level: 3
        docstring_options:
            ignore_init_summary: false
        members:
            - ColumnExistsResult
            - column_exists
            - columns_exists
            - assert_column_exists
            - assert_columns_exists
            - warn_column_missing
            - warn_columns_missing


## Column Values

::: toolbox_pyspark.checks
    options:
        show_root_heading: false
        heading_level: 3
        members:
            - column_contains_value


## Type Checks

::: toolbox_pyspark.checks
    options:
        show_root_heading: false
        heading_level: 3
        members:
            - is_vaid_spark_type
            - assert_valid_spark_type


## Column Types

::: toolbox_pyspark.checks
    options:
        show_root_heading: false
        heading_level: 3
        members:
            - ColumnsAreTypeResult
            - column_is_type
            - columns_are_type
            - assert_column_is_type
            - assert_columns_are_type
            - warn_column_invalid_type
            - warn_columns_invalid_type


## Table Existence

::: toolbox_pyspark.checks
    options:
        show_root_heading: false
        heading_level: 3
        members:
            - table_exists
            - assert_table_exists
