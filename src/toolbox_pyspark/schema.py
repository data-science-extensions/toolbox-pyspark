# ============================================================================ #
#                                                                              #
#     Title   : Schema                                                         #
#     Purpose : Checking, validating, and viewing any schema differences       #
#               between two different tables, either from in-memory variables, #
#               or pointing to locations on disk.                              #
#                                                                              #
# ============================================================================ #


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Overview                                                              ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Description                                                              ####
# ---------------------------------------------------------------------------- #


"""
!!! note "Summary"
    The `schema` module is used for checking, validating, and viewing any schema differences between two different tables, either from in-memory variables, or pointing to locations on disk.
"""


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  Imports                                                                  ####
# ---------------------------------------------------------------------------- #


# ## Python StdLib Imports ----
from pprint import pprint
from typing import Literal, NamedTuple, Optional, Union

# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, SparkSession
from pyspark.sql.types import StructField
from toolbox_python.collection_types import str_list, str_set
from typeguard import typechecked

# ## Local First Party Imports ----
from toolbox_pyspark.io import read_from_path


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "view_schema_differences",
    "check_schemas_match",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Functions                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Classes                                                                 ####
## --------------------------------------------------------------------------- #


class ValidMethods(NamedTuple):
    """
    ```py
    by_table_and_table: str_set
    by_table_and_path: str_set
    by_path_and_table: str_set
    by_path_and_path: str_set
    ```
    """

    by_table_and_table: str_set = {
        "table",
        "table_table",
        "tables",
        "by_table",
        "by_table_and_table",
        "table_and_table",
    }
    """
    ```py
    {
        "table",
        "table_table",
        "tables",
        "by_table",
        "by_table_and_table",
        "table_and_table",
    }
    ```
    """
    by_table_and_path: str_set = {
        "table_and_path",
        "table_path",
        "by_table_and_path",
    }
    """
    ```py
    {
        "table_and_path",
        "table_path",
        "by_table_and_path",
    }
    ```
    """
    by_path_and_table: str_set = {
        "path_and_table",
        "path_table",
        "by_path_and_table",
    }
    """
    ```py
    {
        "path_and_table",
        "path_table",
        "by_path_and_table",
    }
    ```
    """
    by_path_and_path: str_set = {
        "path_and_path",
        "path_path",
        "by_path_and_path",
        "path",
        "paths",
    }
    """
    ```py
    {
        "path_and_path",
        "path_path",
        "by_path_and_path",
        "path",
        "paths",
    }
    ```
    """


# ---------------------------------------------------------------------------- #
#  Check Matching                                                           ####
# ---------------------------------------------------------------------------- #


@typechecked
def _check_schemas_match_by_table_and_table(
    left_table: psDataFrame,
    right_table: psDataFrame,
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    return_object: Literal["results", "check"] = "check",
) -> Union[list, bool]:

    # Set up
    left_schema: dict = left_table.schema.__dict__
    left_names: str_list = left_schema["names"]
    left_fields: list[StructField] = left_schema["fields"]
    right_schema: dict = right_table.schema.__dict__
    right_names: str_list = right_schema["names"]
    right_fields: list[StructField] = right_schema["fields"]
    results = list()

    # Loop for any additions
    if include_add_field:
        for left_field in left_fields:
            if left_field.name not in right_names:
                results.append(("add", {"left": left_field}))

    # Loop for any removals
    if include_remove_field:
        for right_field in right_fields:
            if right_field.name not in left_names:
                results.append(("remove", {"right": right_field}))

    # Loop for any changes
    if include_change_field:
        for left_field in left_fields:
            if left_field.name not in right_names:
                continue
            right_field: StructField = [
                field for field in right_fields if field.name == left_field.name
            ][0]
            if left_field.dataType != right_field.dataType:
                results.append(("change_type", {"left": left_field, "right": right_field}))
            if include_change_nullable:
                if left_field.nullable != right_field.nullable:
                    results.append(
                        ("change_nullable", {"left": left_field, "right": right_field})
                    )

    # Return
    if len(results) > 0:
        if return_object == "results":
            return results
        elif return_object == "check":
            return False
    return True


@typechecked
def _check_schemas_match_by_table_and_path(
    left_table: psDataFrame,
    right_table_path: str,
    right_table_name: str,
    spark_session: SparkSession,
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    return_object: Literal["results", "check"] = "check",
) -> Union[list, bool]:
    right_table: psDataFrame = read_from_path(
        name=right_table_name,
        path=right_table_path,
        spark_session=spark_session,
        data_format=right_table_format,
    )
    return _check_schemas_match_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        return_object=return_object,
    )


@typechecked
def _check_schemas_match_by_path_and_table(
    left_table_path: str,
    left_table_name: str,
    right_table: psDataFrame,
    spark_session: SparkSession,
    left_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    return_object: Literal["results", "check"] = "check",
) -> Union[list, bool]:
    left_table: psDataFrame = read_from_path(
        name=left_table_name,
        path=left_table_path,
        spark_session=spark_session,
        data_format=left_table_format,
    )
    return _check_schemas_match_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        return_object=return_object,
    )


@typechecked
def _check_schemas_match_by_path_and_path(
    left_table_path: str,
    left_table_name: str,
    right_table_path: str,
    right_table_name: str,
    spark_session: SparkSession,
    left_table_format: str = "delta",
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    return_object: Literal["results", "check"] = "check",
) -> Union[list, bool]:
    left_table: psDataFrame = read_from_path(
        name=left_table_name,
        path=left_table_path,
        spark_session=spark_session,
        data_format=left_table_format,
    )
    right_table: psDataFrame = read_from_path(
        name=right_table_name,
        path=right_table_path,
        spark_session=spark_session,
        data_format=right_table_format,
    )
    return _check_schemas_match_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        return_object=return_object,
    )


@typechecked
def check_schemas_match(
    method: str = "by_table_and_table",
    left_table: Optional[psDataFrame] = None,
    right_table: Optional[psDataFrame] = None,
    left_table_path: Optional[str] = None,
    left_table_name: Optional[str] = None,
    right_table_path: Optional[str] = None,
    right_table_name: Optional[str] = None,
    spark_session: Optional[SparkSession] = None,
    left_table_format: str = "delta",
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    return_object: Literal["results", "check"] = "check",
) -> Union[list[tuple[str, dict[str, StructField]]], bool]:
    """
    !!! note "Summary"
        Check the schemas between two different tables.

    ???+ abstract "Details"
        This function is heavily inspired by other packages which check and validate schema differences for `pyspark` tables. This function just streamlines it a bit, and adds additional functionality for whether or not table on either `left` or `right` side is already in-memory or sitting on a directory somewhere else.

    Params:
        method (str, optional):
            The method to use for the comparison. That is, is either side a table in memory or is it a `table` sitting on a `path`?. Check the Notes section for all options available for this parameter.<br>
            Defaults to `#!py "by_table_and_table"`.
        spark_session (Optional[SparkSession], optional):
            The `SparkSession` to use if either the `left` or `right` tables are sitting on a `path` somewhere.<br>
            Defaults to `#!py None`.
        left_table (Optional[psDataFrame], optional):
            If `method` defines the `left` table as a `table`, then this parameter is the actual `dataframe` to do the checking against.<br>
            Defaults to `#!py None`.
        left_table_path (Optional[str], optional):
            If `method` defines the `left` table as a `path`, then this parameter is the actual path location where the table can be found.<br>
            Defaults to `#!py None`.
        left_table_name (Optional[str], optional):
            If `method` defines the `left` table as a `path`, then this parameter is the name of the table found at the given `left_table_path` location.<br>
            Defaults to `#!py None`.
        left_table_format (str, optional):
            If `method` defines the `left` table as a `path`, then this parameter is the format of the table found at the given `left_table_path` location.<br>
            Defaults to `#!py "delta"`.
        right_table (Optional[psDataFrame], optional):
            If `method` defines the `right` table as a `table`, then this parameter is the actual `dataframe` to do the checking against.<br>
            Defaults to `#!py None`.
        right_table_path (Optional[str], optional):
            If `method` defines the `right` table as a `path`, then this parameter is the actual path location where the table can be found.<br>
            Defaults to `#!py None`.
        right_table_name (Optional[str], optional):
            If `method` defines the `right` table as a `path`, then this parameter is the name of the table found at the given `right_table_path` location.<br>
            Defaults to `#!py None`.
        right_table_format (str, optional):
            If `method` defines the `right` table as a `path`, then this parameter is the format of the table found at the given `right_table_path` location.<br>
            Defaults to `#!py "delta"`.
        include_change_field (bool, optional):
            When doing the schema validations, do you want to include any fields where the data-type on the right-hand side is different from the left-hand side?<br>
            This can be read as: "What fields have had their data type _changed **between**_ the left-hand side and the right-hand side?"<br>
            Defaults to `#!py True`.
        include_add_field (bool, optional):
            When doing the schema validations, do you want to include any fields that have had any additional fields added to the left-hand side, when compared to the right-hand side?<br>
            This can be read as: "What fields have been _added **to**_ the left-hand side?"<br>
            Defaults to `#!py True`.
        include_remove_field (bool, optional):
            When doing the schema validations, do you want to include any fields which are missing from the left-hand side and only existing on the right-hand side?<br>
            This can be read as: "What fields been _removed **from**_ the left-hand side?"<br>
            Defaults to `#!py True`.
        include_change_nullable (bool, optional):
            When doing the schema validations, do you want to include any fields which have had their nullability metadata changed on the right-hand side, when compared to the left-hand side?.<br>
            This can be read as: "What fields had their nullability _changed **between**_ the left-hand side and the right-hand side?"<br>
            Defaults to `#!py False`.
        return_object (Literal["results", "check"], optional):
            After having checked the schema, how do you want the results to be returned? If `#!py "check"`, then will only return a `#!py bool` value: `#!py True` if the schemas actually match, `#!py False` if there are any differences. If `#!py "results"`, then the actual schema differences will be returned. Check the Notes section for more information on the structure of this object.<br>
            Defaults to `#!py "check"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the value parse'd to `method` is not a valid option.

    Returns:
        (Union[list[tuple[str, dict[str, StructField]]], bool]):
            If `return_object` is `#!py "results"`, then this will be a `#!py list` of `#!py tuple`'s of `#!py dict`'s containing the details of the schema differences. If `return_object` is `#!py "check"`, then it will only be a `#!py bool` object about whether the schemas match or not.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> from pprint import pprint
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession, functions as F
        >>> from toolbox_pyspark.schema import check_schemas_match
        >>> from toolbox_pyspark.io import write_to_path
        >>> from toolbox_pyspark.checks import table_exists
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df1 = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ["1", "1", "1", "1"],
        ...             "d": ["2", "2", "2", "2"],
        ...             "e": ["3", "3", "3", "3"],
        ...             "f": ["4", "4", "4", "4"],
        ...         }
        ...     )
        ... )
        >>> df2 = (
        ...     df1.withColumn("c", F.col("c").cast("int"))
        ...     .withColumn("g", F.lit("a"))
        ...     .withColumn("d", F.lit("null"))
        ...     .drop("e")
        ... )
        >>> write_to_path(
        ...     table=df1,
        ...     name="left",
        ...     path="./test",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ...     write_options={"overwriteSchema": "true"},
        ... )
        >>> write_to_path(
        ...     table=df2,
        ...     name="right",
        ...     path="./test",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ...     write_options={"overwriteSchema": "true"},
        ... )
        >>>
        >>> # Check
        >>> pprint(df1.dtypes)
        >>> print(df1.show())
        >>> print(table_exists("left", "./test", "parquet", spark))
        >>> pprint(df2.dtypes)
        >>> print(df2.show())
        >>> print(table_exists("right", "./test", "parquet", spark))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "string"),
            ("d", "string"),
            ("e", "string"),
            ("f", "string"),
        ]
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+---+
        | a | b | c | d | e | f |
        +---+---+---+---+---+---+
        | 0 | a | 1 | 2 | 3 | 4 |
        | 1 | b | 1 | 2 | 3 | 4 |
        | 2 | c | 1 | 2 | 3 | 4 |
        | 3 | d | 1 | 2 | 3 | 4 |
        +---+---+---+---+---+---+
        ```
        ```{.sh .shell title="Terminal"}
        True
        ```
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "int"),
            ("d", "string"),
            ("f", "string"),
            ("g", "string"),
        ]
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+------+---+---+
        | a | b | c |    d | f | g |
        +---+---+---+------+---+---+
        | 0 | a | 1 | null | 4 | 2 |
        | 1 | b | 1 | null | 4 | 2 |
        | 2 | c | 1 | null | 4 | 2 |
        | 3 | d | 1 | null | 4 | 2 |
        +---+---+---+------+---+---+
        ```
        ```{.sh .shell title="Terminal"}
        True
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Check matching"}
        >>> diff = check_schemas_match(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df1,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     return_object="check",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        True
        ```
        !!! success "Conclusion: Schemas match."
        </div>

        ```{.py .python linenums="1" title="Example 2: Check not matching"}
        >>> diff = check_schemas_match(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     return_object="check",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        False
        ```
        !!! failure "Conclusion: Schemas do not match."
        </div>

        ```{.py .python linenums="1" title="Example 3: Show only `add`"}
        >>> diff = check_schemas_match(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=False,
        ...     include_remove_field=False,
        ...     include_change_nullable=False,
        ...     return_object="results",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            (
                "add",
                {"left": T.StructField("e", T.StringType(), False)},
            ),
        ]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added."
        </div>

        ```{.py .python linenums="1" title="Example 4: Show `add` and `remove`"}
        >>> diff = check_schemas_match(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=False,
        ...     include_remove_field=True,
        ...     include_change_nullable=False,
        ...     return_object="results",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            (
                "add",
                {"left": T.StructField("e", T.StringType(), False)},
            ),
            (
                "remove",
                {"right": T.StructField("g", T.StringType(), False)},
            ),
        ]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added and the `g` field was removed."
        </div>

        ```{.py .python linenums="1" title="Example 5: Show all changes"}
        >>> diff = check_schemas_match(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     return_object="results",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
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
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added, the `g` field was removed, the `c` field had its data type changed, and the `c` field had its nullability changed."
        </div>

        ```{.py .python linenums="1" title="Example 6: Check where right-hand side is a `path`"}
        >>> diff = check_schemas_match(
        ...     method="path_table",
        ...     spark_session=spark,
        ...     left_table=df1,
        ...     right_table_path="./test",
        ...     right_table_name="right",
        ...     right_table_format="parquet",
        ...     include_add_field=True,
        ...     include_change_field=False,
        ...     include_remove_field=False,
        ...     include_change_nullable=False,
        ...     return_object="results",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            (
                "add",
                {"left": T.StructField("e", T.StringType(), False)},
            ),
        ]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added."
        </div>

        ```{.py .python linenums="1" title="Example 7: Check where both sides are a `path`"}
        >>> diff = check_schemas_match(
        ...     method="path_path",
        ...     spark_session=spark,
        ...     left_table_path="./test",
        ...     left_table_name="left",
        ...     left_table_format="parquet",
        ...     right_table_path="./test",
        ...     right_table_name="right",
        ...     right_table_format="parquet",
        ...     include_add_field=False,
        ...     include_change_field=True,
        ...     include_remove_field=False,
        ...     include_change_nullable=False,
        ...     return_object="results",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            (
                "remove",
                {"right": T.StructField("g", T.StringType(), True)},
            ),
        ]
        ```
        !!! failure "Conclusion: Schemas do not match because the `g` field was removed."
        </div>

        ```{.py .python linenums="1" title="Example 8: Invalid `method` parameter"}
        >>> diff = check_schemas_match(
        ...     method="invalid",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     return_object="check",
        ... )
        ```
        <div class="result" markdown>
        ```{.py .python .title="Terminal"}
        AttributeError: Invalid value for `method`: 'invalid'
        Please use one of the following options:
        - For `by_table_and_table`, use one of the following values: ['table', 'table_table', 'tables', 'by_table', 'by_table_and_table', 'table_and_table']
        - For `by_table_and_path`, use one of the following values: ['table_and_path', 'table_path', 'by_table_and_path']
        - For `by_path_and_table`, use one of the following values: ['path_and_table', 'path_table', 'by_path_and_table']
        - For `by_path_and_path`, use one of the following values: ['path_and_path', 'path_path', 'by_path_and_path', 'path', 'paths']
        ```
        !!! failure "Conclusion: Invalid `method` parameter."
        </div>

    ???+ info "Notes"

        ???+ info "Options available in the `method` parameter"

            The options available in the `method` parameter include:

            - If the objects on both the left-hand side and the right-hand side are both `dataframes` already loaded to memory, use one of the following values:
                <div class="mdx-three-columns" markdown>
                - `#!py "table"`
                - `#!py "table_table"`
                - `#!py "tables"`
                - `#!py "by_table"`
                - `#!py "by_table_and_table"`
                - `#!py "table_and_table"`
                </div>
            - If the object on the left-hand side is a `dataframe` already loaded to memory, but the object on the right-hand side is a table sitting on a path somewhere, use one of the following values:
                <div class="mdx-three-columns" markdown>
                - `#!py "table_and_path"`
                - `#!py "table_path"`
                - `#!py "by_table_and_path"`
                </div>
            - If the object on the left-hand side is a table sitting on a path somewhere, but the object on the right-hand side is a `dataframe` already loaded to memory, use one of the following values:
                <div class="mdx-three-columns" markdown>
                - `#!py "path_and_table"`
                - `#!py "path_table"`
                - `#!py "by_path_and_table"`
                </div>
            - If the objects on both the left-hand side and the right-hand side are both tables sitting on a path somewhere, then use one of the following values:
                <div class="mdx-three-columns" markdown>
                - `#!py "path_and_path"`
                - `#!py "path_path"`
                - `#!py "by_path_and_path"`
                - `#!py "path"`
                - `#!py "paths"`
                </div>

        ???+ info "Details about the return object when we set the parameter `#!py return_object="results"`"

            - When we set the parameter `#!py return_object="results"`, then we will get an object returned from this function.
            - That object will be a `#!py list` of `#!py tuple`'s, each `#!py tuple` is only two-elements long, where the first element is a `#!py str` object, and the second is a `#!py dict` where the keys are `#!py str` and the values are a `#!py StructField` object.
            - For each of the `#!py tuple` elements, the first element (the `#!py str` object) describes what the `#!py tuple` is there for. It will be one of four words: `#!py "add"`, `#!py "remove"`, `#!py "change_type"`, or `#!py "change_nullable"`.
            - You can change whether these options are included in the schema check by changing the other parameters: `#!py include_change_field`, `#!py include_add_field`, `#!py include_remove_field`, `#!py include_change_nullable`.
            - The structure of the list will look like this:

            ```{.py .python .title="The structure of the returned object"}
            [
                (
                    "add",  # (1)!
                    {"left": T.StructField("e", T.StringType(), False)},  # (2)!
                ),
                (
                    "add",  # (3)!
                    {"left": T.StructField("h", T.StringType(), False)},
                ),
                (
                    "remove",  # (4)!
                    {"right": T.StructField("g", T.StringType(), False)},  # (5)!
                ),
                (
                    "change_type",  # (6)!
                    {
                        "left": T.StructField("c", T.StringType(), False),  # (7)!
                        "right": T.StructField("c", T.IntegerType(), True),
                    },
                ),
                (
                    "change_nullable",  # (8)!
                    {
                        "left": T.StructField("c", T.StringType(), False),  # (9)!
                        "right": T.StructField("c", T.IntegerType(), True),
                    },
                ),
            ]
            ```

            1. When `#!py include_add_field=True`, then the `add` section will always appear first.<br>
                If `#!py include_add_field=False`, then this section is omitted.
            2. The second element of the `#!py tuple` is a `#!py dict` that has only one `key`-`value` pair.<br>
                The `key` will _always_ be the value `#!py "left"`, because these are fields which have been added to the table on the left-hand side and not found on the right-hand side.
            3. When there are multiple fields added to the table on the left-hand side, they will appear like this.
            4. When `#!py include_remove_field=True`, then the `remove` section will always appear next.<br>
                If `#!py include_remove_field=False`, then this section is omitted.
            5. The second element of the `#!py tuple` is a `#!py dict` that has only one `key`-`value` pair.<br>
                The `key` will _always_ be the value `#!py "right"`, because these are fields which have been removed from the left-hand side and only visible on the right-hand side.
            6. When `#!py include_change_field=True`, then the `change_type` section will always appear next.<br>
                If `#!py include_change_field=False`, then this section is omitted.
            7. The second element of the `#!py tuple` is a `#!py dict` that has two `key`-`value` pairs.<br>
                The `key`'s will _always_ be the values `#!py "left"` then `#!py "right"`, because these are fields where the data type has changed between the left-hand side and the right-hand side, and therefore you need to see both to see exactly what has changed.
            8. When `#!py include_change_nullable=True`, then the `change_nullable` section will always appear next.<br>
                If `#!py include_change_nullable=False`, then this section is omitted.
            9. The sectond element of the `#!py tuple` is a `#!py dict` that has two `key`-`value` pairs.<br>
                The `key`'s will _always_ be the values `#!py "left"` then `#!py "right"`, because these are fields where the nullability of the firlds are changed between the left-hand side and the right-hand side, and therefore you need to see both to see exactly what has changed.
    """

    valid_methods = ValidMethods()
    msg: str = "If using the '{meth}' method, then '{name}' cannot be 'None'."

    if method in valid_methods.by_table_and_table:
        assert left_table is not None, msg.format(meth=method, name="left_table")
        assert right_table is not None, msg.format(meth=method, name="right_table")
        return _check_schemas_match_by_table_and_table(
            left_table=left_table,
            right_table=right_table,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            return_object=return_object,
        )
    elif method in valid_methods.by_table_and_path:
        assert left_table is not None, msg.format(meth=method, name="left_table")
        assert right_table_path is not None, msg.format(meth=method, name="right_table_path")
        assert right_table_name is not None, msg.format(meth=method, name="right_table_name")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _check_schemas_match_by_table_and_path(
            left_table=left_table,
            right_table_path=right_table_path,
            right_table_name=right_table_name,
            right_table_format=right_table_format,
            spark_session=spark_session,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            return_object=return_object,
        )
    elif method in valid_methods.by_path_and_table:
        assert left_table_path is not None, msg.format(meth=method, name="left_table_path")
        assert left_table_name is not None, msg.format(meth=method, name="left_table_name")
        assert right_table is not None, msg.format(meth=method, name="right_table")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _check_schemas_match_by_path_and_table(
            left_table_path=left_table_path,
            left_table_name=left_table_name,
            right_table=right_table,
            spark_session=spark_session,
            left_table_format=left_table_format,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            return_object=return_object,
        )
    elif method in valid_methods.by_path_and_path:
        assert left_table_path is not None, msg.format(meth=method, name="left_table_path")
        assert left_table_name is not None, msg.format(meth=method, name="left_table_name")
        assert right_table_path is not None, msg.format(meth=method, name="right_table_path")
        assert right_table_name is not None, msg.format(meth=method, name="right_table_name")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _check_schemas_match_by_path_and_path(
            left_table_path=left_table_path,
            left_table_name=left_table_name,
            left_table_format=left_table_format,
            right_table_path=right_table_path,
            right_table_name=right_table_name,
            right_table_format=right_table_format,
            spark_session=spark_session,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            return_object=return_object,
        )
    else:
        raise AttributeError(
            f"Invalid value for `method`: '{method}'\n"
            f"Please use one of the following options:\n"
            f"- For `by_table_and_table`, use one of: {valid_methods.by_table_and_table}\n"
            f"- For `by_table_and_path`, use one of: {valid_methods.by_table_and_path}\n"
            f"- For `by_path_and_table`, use one of: {valid_methods.by_path_and_table}\n"
            f"- For `by_path_and_path`, use one of: {valid_methods.by_path_and_path}\n"
        )


# ---------------------------------------------------------------------------- #
#  View Differences                                                         ####
# ---------------------------------------------------------------------------- #


@typechecked
def _view_schema_differences_by_table_and_table(
    left_table: psDataFrame,
    right_table: psDataFrame,
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    view_type: Literal["print", "pprint", "return"] = "pprint",
) -> Optional[Union[list[tuple[str, dict[str, StructField]]], bool]]:
    schema_differences: Union[list[tuple[str, dict[str, StructField]]], bool] = (
        check_schemas_match(
            method="table_table",
            left_table=left_table,
            right_table=right_table,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            return_object="results",
        )
    )
    if isinstance(schema_differences, list) and len(schema_differences) > 0:
        if view_type == "print":
            print(schema_differences)
        elif view_type == "pprint":
            pprint(schema_differences)
        elif view_type == "return":
            return schema_differences
    return None


@typechecked
def _view_schema_differences_by_path_and_path(
    left_table_path: str,
    left_table_name: str,
    right_table_path: str,
    right_table_name: str,
    spark_session: SparkSession,
    left_table_format: str = "delta",
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    view_type: Literal["print", "pprint", "return"] = "pprint",
) -> Optional[Union[list[tuple[str, dict[str, StructField]]], bool]]:
    left_table: psDataFrame = read_from_path(
        name=left_table_name,
        path=left_table_path,
        spark_session=spark_session,
        data_format=left_table_format,
    )
    right_table: psDataFrame = read_from_path(
        name=right_table_name,
        path=right_table_path,
        spark_session=spark_session,
        data_format=right_table_format,
    )
    return _view_schema_differences_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        view_type=view_type,
    )


@typechecked
def _view_schema_differences_by_table_and_path(
    left_table: psDataFrame,
    right_table_path: str,
    right_table_name: str,
    spark_session: SparkSession,
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    view_type: Literal["print", "pprint", "return"] = "pprint",
) -> Optional[Union[list[tuple[str, dict[str, StructField]]], bool]]:
    right_table: psDataFrame = read_from_path(
        name=right_table_name,
        path=right_table_path,
        spark_session=spark_session,
        data_format=right_table_format,
    )
    return _view_schema_differences_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        view_type=view_type,
    )


@typechecked
def _view_schema_differences_by_path_and_table(
    left_table_path: str,
    left_table_name: str,
    right_table: psDataFrame,
    spark_session: SparkSession,
    left_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    view_type: Literal["print", "pprint", "return"] = "pprint",
) -> Optional[Union[list[tuple[str, dict[str, StructField]]], bool]]:
    left_table: psDataFrame = read_from_path(
        name=left_table_name,
        path=left_table_path,
        spark_session=spark_session,
        data_format=left_table_format,
    )
    return _view_schema_differences_by_table_and_table(
        left_table=left_table,
        right_table=right_table,
        include_change_field=include_change_field,
        include_add_field=include_add_field,
        include_remove_field=include_remove_field,
        include_change_nullable=include_change_nullable,
        view_type=view_type,
    )


@typechecked
def view_schema_differences(
    method: str = "by_table_and_table",
    spark_session: Optional[SparkSession] = None,
    left_table: Optional[psDataFrame] = None,
    left_table_path: Optional[str] = None,
    left_table_name: Optional[str] = None,
    left_table_format: str = "delta",
    right_table: Optional[psDataFrame] = None,
    right_table_path: Optional[str] = None,
    right_table_name: Optional[str] = None,
    right_table_format: str = "delta",
    include_change_field: bool = True,
    include_add_field: bool = True,
    include_remove_field: bool = True,
    include_change_nullable: bool = False,
    view_type: Literal["print", "pprint", "return"] = "pprint",
) -> Optional[Union[list[tuple[str, dict[str, StructField]]], bool]]:
    """
    !!! note "Summary"
        View the schemas between two different tables.

    ???+ abstract "Details"
        The primary differences between [`check_schemas_match()`][toolbox_pyspark.schema.check_schemas_match] and [`view_schema_differences()`][toolbox_pyspark.schema.view_schema_differences] is that [`check_...()`][toolbox_pyspark.schema.check_schemas_match] returns either a `#!py bool` result, or the actual details of the schema differences, whilst [`view_...()`][toolbox_pyspark.schema.view_schema_differences] may also return the actual details object, but it will also print the result to the terminal for you to review.<br>
        For full details of all the parameters and all the options, including nuances and detailed explanations and thorough examples, please check the [`check_schemas_match()`][toolbox_pyspark.schema.check_schemas_match] function.

    Params:
        method (str, optional):
            The method to use for the comparison. That is, is either side a table in memory or is it a `table` sitting on a `path`?. Check the Notes section for all options available for this parameter.<br>
            Defaults to `#!py "by_table_and_table"`.
        spark_session (Optional[SparkSession], optional):
            The `SparkSession` to use if either the `left` or `right` tables are sitting on a `path` somewhere.<br>
            Defaults to `#!py None`.
        left_table (Optional[psDataFrame], optional):
            If `method` defines the `left` table as a `table`, then this parameter is the actual `dataframe` to do the checking against.<br>
            Defaults to `#!py None`.
        left_table_path (Optional[str], optional):
            If `method` defines the `left` table as a `path`, then this parameter is the actual path location where the table can be found.<br>
            Defaults to `#!py None`.
        left_table_name (Optional[str], optional):
            If `method` defines the `left` table as a `path`, then this parameter is the name of the table found at the given `left_table_path` location.<br>
            Defaults to `#!py None`.
        left_table_format (str, optional):
            If `method` defines the `left` table as a `path`, then this parameter is the format of the table found at the given `left_table_path` location.<br>
            Defaults to `#!py "delta"`.
        right_table (Optional[psDataFrame], optional):
            If `method` defines the `right` table as a `table`, then this parameter is the actual `dataframe` to do the checking against.<br>
            Defaults to `#!py None`.
        right_table_path (Optional[str], optional):
            If `method` defines the `right` table as a `path`, then this parameter is the actual path location where the table can be found.<br>
            Defaults to `#!py None`.
        right_table_name (Optional[str], optional):
            If `method` defines the `right` table as a `path`, then this parameter is the name of the table found at the given `right_table_path` location.<br>
            Defaults to `#!py None`.
        right_table_format (str, optional):
            If `method` defines the `right` table as a `path`, then this parameter is the format of the table found at the given `right_table_path` location.<br>
            Defaults to `#!py "delta"`.
        include_change_field (bool, optional):
            When doing the schema validations, do you want to include any fields where the data-type on the right-hand side is different from the left-hand side?<br>
            This can be read as: "What fields have had their data type _changed **between**_ the left-hand side and the right-hand side?"<br>
            Defaults to `#!py True`.
        include_add_field (bool, optional):
            When doing the schema validations, do you want to include any fields that have had any additional fields added to the left-hand side, when compared to the right-hand side?<br>
            This can be read as: "What fields have been _added **to**_ the left-hand side?"<br>
            Defaults to `#!py True`.
        include_remove_field (bool, optional):
            When doing the schema validations, do you want to include any fields which are missing from the left-hand side and only existing on the right-hand side?<br>
            This can be read as: "What fields been _removed **from**_ the left-hand side?"<br>
            Defaults to `#!py True`.
        include_change_nullable (bool, optional):
            When doing the schema validations, do you want to include any fields which have had their nullability metadata changed on the right-hand side, when compared to the left-hand side?.<br>
            This can be read as: "What fields had their nullability _changed **between**_ the left-hand side and the right-hand side?"<br>
            Defaults to `#!py False`.
        view_type (Literal["print", "pprint", "return"], optional):
            When returning the output from this function, how do you want it to be displayed? Must be one of `#!py ["print", "pprint", "return"]`.<br>
            Logically, the difference is that `#!py "print"` will display a text value to the terminal that is not formatted in any way; `#!py "pprint"` will display a pretty-printed text value to the terminal; and `#!py "return"` will return the schema differences which can then be assigned to another variable.<br>
            Defaults to `#!py "pprint"`.

    Raises:
        TypeError:
            If any of the inputs parsed to the parameters of this function are not the correct type. Uses the [`@typeguard.typechecked`](https://typeguard.readthedocs.io/en/stable/api.html#typeguard.typechecked) decorator.
        AttributeError:
            If the value parse'd to `method` is not a valid option.

    Returns:
        (Optional[list[tuple[str, dict[str, StructField]]]]):
            If `#!py view_type="return"`, then this will be a `#!py list` of `#!py tuple`'s of `#!py dict`'s containing the details of the schema differences. If `#!py view_type!="return"` (or if `#!py view_type="return"`, but there are actually no differences in the schema), then nothing is returned; only printed to terminal.

    ???+ example "Examples"

        ```{.py .python linenums="1" title="Set up"}
        >>> # Imports
        >>> from pprint import pprint
        >>> import pandas as pd
        >>> from pyspark.sql import SparkSession, functions as F
        >>> from toolbox_pyspark.schema import view_schema_differences
        >>> from toolbox_pyspark.io import write_to_path
        >>> from toolbox_pyspark.checks import table_exists
        >>>
        >>> # Instantiate Spark
        >>> spark = SparkSession.builder.getOrCreate()
        >>>
        >>> # Create data
        >>> df1 = spark.createDataFrame(
        ...     pd.DataFrame(
        ...         {
        ...             "a": [0, 1, 2, 3],
        ...             "b": ["a", "b", "c", "d"],
        ...             "c": ["1", "1", "1", "1"],
        ...             "d": ["2", "2", "2", "2"],
        ...             "e": ["3", "3", "3", "3"],
        ...             "f": ["4", "4", "4", "4"],
        ...         }
        ...     )
        ... )
        >>> df2 = (
        ...     df1.withColumn("c", F.col("c").cast("int"))
        ...     .withColumn("g", F.lit("a"))
        ...     .withColumn("d", F.lit("null"))
        ...     .drop("e")
        ... )
        >>> write_to_path(
        ...     table=df1,
        ...     name="left",
        ...     path="./test",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ...     write_options={"overwriteSchema": "true"},
        ... )
        >>> write_to_path(
        ...     table=df2,
        ...     name="right",
        ...     path="./test",
        ...     data_format="parquet",
        ...     mode="overwrite",
        ...     write_options={"overwriteSchema": "true"},
        ... )
        >>>
        >>> # Check
        >>> pprint(df1.dtypes)
        >>> print(df1.show())
        >>> print(table_exists("left", "./test", "parquet", spark))
        >>> pprint(df2.dtypes)
        >>> print(df2.show())
        >>> print(table_exists("right", "./test", "parquet", spark))
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "string"),
            ("d", "string"),
            ("e", "string"),
            ("f", "string"),
        ]
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+---+---+---+
        | a | b | c | d | e | f |
        +---+---+---+---+---+---+
        | 0 | a | 1 | 2 | 3 | 4 |
        | 1 | b | 1 | 2 | 3 | 4 |
        | 2 | c | 1 | 2 | 3 | 4 |
        | 3 | d | 1 | 2 | 3 | 4 |
        +---+---+---+---+---+---+
        ```
        ```{.sh .shell title="Terminal"}
        True
        ```
        ```{.sh .shell title="Terminal"}
        [
            ("a", "bigint"),
            ("b", "string"),
            ("c", "int"),
            ("d", "string"),
            ("f", "string"),
            ("g", "string"),
        ]
        ```
        ```{.txt .text title="Terminal"}
        +---+---+---+------+---+---+
        | a | b | c |    d | f | g |
        +---+---+---+------+---+---+
        | 0 | a | 1 | null | 4 | 2 |
        | 1 | b | 1 | null | 4 | 2 |
        | 2 | c | 1 | null | 4 | 2 |
        | 3 | d | 1 | null | 4 | 2 |
        +---+---+---+------+---+---+
        ```
        ```{.sh .shell title="Terminal"}
        True
        ```
        </div>

        ```{.py .python linenums="1" title="Example 1: Check matching"}
        >>> view_schema_differences(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df1,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     view_type="return",
        ... )
        >>> print(diff)
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        None
        ```
        !!! success "Conclusion: Schemas match."
        </div>

        ```{.py .python linenums="1" title="Example 2: Check print"}
        >>> view_schema_differences(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=False,
        ...     include_remove_field=False,
        ...     include_change_nullable=False,
        ...     view_type="print",
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [('add', {'left': StructField('e', StringType(), True)})]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added."
        </div>

        ```{.py .python linenums="1" title="Example 3: Check pprint"}
        >>> view_schema_differences(
        ...     method="table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     view_type="pprint",
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [('add', {'left': StructField('e', StringType(), False)}),
         ('remove', {'right': StructField('g', StringType(), False)}),
         ('change_type',
          {'left': StructField('c', StringType(), False),
           'right': StructField('c', IntegerType(), True)}),
         ('change_nullable',
          {'left': StructField('c', StringType(), False),
           'right': StructField('c', IntegerType(), True)})]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added, the `g` field was removed, the `c` field had its data type changed, and the `c` field had its nullability changed."
        </div>

        ```{.py .python linenums="1" title="Example 4: Check with right-hand side as a `path`"}
        >>> view_schema_differences(
        ...     method="table_table",
        ...     spark_session=spark,
        ...     left_table=df1,
        ...     right_table_path="./test",
        ...     right_table_name="right",
        ...     right_table_format="parquet",
        ...     include_add_field=True,
        ...     include_change_field=False,
        ...     include_remove_field=False,
        ...     include_change_nullable=False,
        ...     view_type="pprint",
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [('add', {'left': StructField('e', StringType(), True)})]
        ```
        !!! failure "Conclusion: Schemas do not match because the `e` field was added."
        </div>

        ```{.py .python linenums="1" title="Example 5: Check with both sides being a `path`"}
        >>> view_schema_differences(
        ...     method="table_table",
        ...     spark_session=spark,
        ...     left_table_path="./test",
        ...     left_table_name="left",
        ...     left_table_format="parquet",
        ...     right_table_path="./test",
        ...     right_table_name="right",
        ...     right_table_format="parquet",
        ...     include_add_field=False,
        ...     include_change_field=False,
        ...     include_remove_field=True,
        ...     include_change_nullable=False,
        ...     view_type="pprint",
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell title="Terminal"}
        [('remove', {'right': StructField('g', StringType(), True)})]
        ```
        !!! failure "Conclusion: Schemas do not match because the `g` field was removed."
        </div>

        ```{.py .python linenums="1" title="Example 6: Invalid `method` parameter"}
        >>> view_schema_differences(
        ...     method="table_table_table",
        ...     left_table=df1,
        ...     right_table=df2,
        ...     include_add_field=True,
        ...     include_change_field=True,
        ...     include_remove_field=True,
        ...     include_change_nullable=True,
        ...     view_type="return",
        ... )
        ```
        <div class="result" markdown>
        ```{.sh .shell  title="Terminal"}
        AttributeError: Invalid value for `method`: 'table_table_table'
        Please use one of the following options:
        - For `by_table_and_table`, use one of the following values: ['table', 'table_table', 'tables', 'by_table', 'by_table_and_table', 'table_and_table']
        - For `by_table_and_path`, use one of the following values: ['table_and_path', 'table_path', 'by_table_and_path']
        - For `by_path_and_table`, use one of the following values: ['path_and_table', 'path_table', 'by_path_and_table']
        - For `by_path_and_path`, use one of the following values: ['path_and_path', 'path_path', 'by_path_and_path', 'path', 'paths']
        ```
        !!! failure "Conclusion: Invalid `method` parameter."
        </div>

    ??? tip "See Also"
        - [`check_schemas_match()`][toolbox_pyspark.schema.check_schemas_match]
    """

    valid_methods: ValidMethods = ValidMethods()
    msg: str = "If using the '{meth}' method, then '{name}' cannot be 'None'."

    if method in valid_methods.by_table_and_table:
        assert left_table is not None, msg.format(meth=method, name="left_table")
        assert right_table is not None, msg.format(meth=method, name="right_table")
        return _view_schema_differences_by_table_and_table(
            left_table=left_table,
            right_table=right_table,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            view_type=view_type,
        )
    elif method in valid_methods.by_table_and_path:
        assert left_table is not None, msg.format(meth=method, name="left_table")
        assert right_table_path is not None, msg.format(meth=method, name="right_table_path")
        assert right_table_name is not None, msg.format(meth=method, name="right_table_name")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _view_schema_differences_by_table_and_path(
            left_table=left_table,
            right_table_path=right_table_path,
            right_table_name=right_table_name,
            right_table_format=right_table_format,
            spark_session=spark_session,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            view_type=view_type,
        )
    elif method in valid_methods.by_path_and_table:
        assert left_table_path is not None, msg.format(meth=method, name="left_table_path")
        assert left_table_name is not None, msg.format(meth=method, name="left_table_name")
        assert right_table is not None, msg.format(meth=method, name="right_table")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _view_schema_differences_by_path_and_table(
            left_table_path=left_table_path,
            left_table_name=left_table_name,
            left_table_format=left_table_format,
            right_table=right_table,
            spark_session=spark_session,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            view_type=view_type,
        )
    elif method in valid_methods.by_path_and_path:
        assert left_table_path is not None, msg.format(meth=method, name="left_table_path")
        assert left_table_name is not None, msg.format(meth=method, name="left_table_name")
        assert right_table_path is not None, msg.format(meth=method, name="right_table_path")
        assert right_table_name is not None, msg.format(meth=method, name="right_table_name")
        assert spark_session is not None, msg.format(meth=method, name="spark_session")
        return _view_schema_differences_by_path_and_path(
            left_table_path=left_table_path,
            left_table_name=left_table_name,
            left_table_format=left_table_format,
            right_table_path=right_table_path,
            right_table_name=right_table_name,
            right_table_format=right_table_format,
            spark_session=spark_session,
            include_change_field=include_change_field,
            include_add_field=include_add_field,
            include_remove_field=include_remove_field,
            include_change_nullable=include_change_nullable,
            view_type=view_type,
        )
    else:
        raise AttributeError(
            f"Invalid value for `method`: '{method}'\n"
            f"Please use one of the following options:\n"
            f"- For `by_table_and_table`, use one of: {valid_methods.by_table_and_table}\n"
            f"- For `by_table_and_path`, use one of: {valid_methods.by_table_and_path}\n"
            f"- For `by_path_and_table`, use one of: {valid_methods.by_path_and_table}\n"
            f"- For `by_path_and_path`, use one of: {valid_methods.by_path_and_path}\n"
        )
