# ============================================================================ #
#                                                                              #
#     Title: Title                                                             #
#     Purpose: Purpose                                                         #
#                                                                              #
# ============================================================================ #


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Setup                                                                 ####
#                                                                              #
# ---------------------------------------------------------------------------- #


## --------------------------------------------------------------------------- #
##  Imports                                                                 ####
## --------------------------------------------------------------------------- #


# ## Python Third Party Imports ----
from pyspark.sql import DataFrame as psDataFrame, functions as F
from toolbox_python.collection_types import str_list


## --------------------------------------------------------------------------- #
##  Exports                                                                 ####
## --------------------------------------------------------------------------- #


__all__: str_list = [
    "format_numbers",
    "display_intermediary_table",
    "display_intermediary_schema",
    "display_intermediary_columns",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Main Section                                                          ####
#                                                                              #
# ---------------------------------------------------------------------------- #


def format_numbers(dataframe: psDataFrame) -> psDataFrame:
    for col, typ in dataframe.dtypes:
        if typ in ("int", "tinyint", "smallint", "bigint"):
            dataframe = dataframe.withColumn(col, F.format_number(col, 0))
        elif typ in ("float", "double"):
            dataframe = dataframe.withColumn(col, F.format_number(col, 2))
    return dataframe


def display_intermediary_table(
    dataframe: psDataFrame, reformat_numbers: bool = True, num_rows: int = 20
) -> psDataFrame:
    if reformat_numbers:
        dataframe = format_numbers(dataframe)
    dataframe.show(n=num_rows, truncate=False)
    return dataframe


def display_intermediary_schema(dataframe: psDataFrame) -> psDataFrame:
    dataframe.printSchema()
    return dataframe


def display_intermediary_columns(dataframe: psDataFrame) -> psDataFrame:
    print(dataframe.columns)
    return dataframe
