# ============================================================================ #
#                                                                              #
#     Title   : Constants                                                      #
#     Purpose : Hold the definitions of all constant values used across the    #
#               package.                                                       #
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
    The `constants` module is used to hold the definitions of all constant values used across the package.
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
from functools import partial
from typing import Literal, Union, get_args
from warnings import warn

# ## Python Third Party Imports ----
from pyspark.sql import types as T
from pyspark.sql.types import _all_atomic_types as pyspark_atomic_types
from toolbox_python.collection_types import str_list, str_set

# ## Local First Party Imports ----
from toolbox_pyspark.utils.whitespaces import WhitespaceCharacters, WhitespaceChatacter


# ---------------------------------------------------------------------------- #
#  Exports                                                                  ####
# ---------------------------------------------------------------------------- #


__all__: str_list = [
    "ALL_WHITESPACE_CHARACTERS",
    "WHITESPACE_CHARACTERS",
    "VALID_PYSPARK_TYPES",
    "VALID_PYSPARK_TYPE_NAMES",
    "ALL_PYSPARK_TYPES",
    "VALID_PYAPARK_JOIN_TYPES",
    "ALL_PYSPARK_JOIN_TYPES",
    "LITERAL_PANDAS_DATAFRAME_NAMES",
    "LITERAL_PYSPARK_DATAFRAME_NAMES",
    "LITERAL_NUMPY_ARRAY_NAMES",
    "LITERAL_LIST_OBJECT_NAMES",
    "VALID_PANDAS_DATAFRAME_NAMES",
    "VALID_PYSPARK_DATAFRAME_NAMES",
    "VALID_NUMPY_ARRAY_NAMES",
    "VALID_LIST_OBJECT_NAMES",
    "VALID_DATAFRAME_NAMES",
    "_DEFAULT_DEPRECATION_WARNING_CLASS",
    "_DEFAULT_DEPRECATION_WARNING",
]


# ---------------------------------------------------------------------------- #
#                                                                              #
#     Constants                                                             ####
#                                                                              #
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#  White Spaces                                                             ####
# ---------------------------------------------------------------------------- #


# For full list of characters: https://en.wikipedia.org/wiki/Whitespace_character
# in the below tuples: ('name','unicode','ascii')
ALL_WHITESPACE_CHARACTERS: list[tuple[str, str, int]] = [
    ("character tabulation", "U+0009", 9),
    ("line feed", "U+000A", 10),
    ("line tabulation", "U+000B", 11),
    ("form feed", "U+000C", 12),
    ("carriage return", "U+000D", 13),
    ("space", "U+0020", 32),
    ("next line", "U+0085", 133),
    ("no-break space", "U+00A0", 160),
    ("ogham space mark", "U+1680", 5760),
    ("en quad", "U+2000", 8192),
    ("em quad", "U+2001", 8193),
    ("en space", "U+2002", 8194),
    ("em space", "U+2003", 8195),
    ("three-per-em space", "U+2004", 8196),
    ("four-per-em space", "U+2005", 8197),
    ("six-per-em space", "U+2006", 8198),
    ("figure space", "U+2007", 8199),
    ("punctuation space", "U+2008", 8200),
    ("thin space", "U+2009", 8201),
    ("hair space", "U+200A", 8202),
    ("line separator", "U+2028", 8232),
    ("paragraph separator", "U+2029", 8233),
    ("narrow no-break space", "U+202F", 8239),
    ("medium mathematical space", "U+205F", 8287),
    ("ideographic space", "U+3000", 12288),
    ("mongolian vowel separator", "U+180E", 6158),
    ("zero width space", "U+200B", 8203),
    ("zero width non-joiner", "U+200C", 8204),
    ("zero width joiner", "U+200D", 8205),
    ("word joiner", "U+2060", 8288),
    ("zero width non-breaking space", "U+FEFF", 65279),
]

WHITESPACE_CHARACTERS = WhitespaceCharacters(
    [
        WhitespaceChatacter(name, unicode, ascii)
        for name, unicode, ascii in ALL_WHITESPACE_CHARACTERS
    ]
)


# ---------------------------------------------------------------------------- #
#  PySpark Types                                                            ####
# ---------------------------------------------------------------------------- #


# For a full list of valid types, see: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
VALID_PYSPARK_TYPES = list(pyspark_atomic_types.values())
VALID_PYSPARK_TYPE_NAMES: str_list = sorted(
    list(pyspark_atomic_types.keys()) + ["str", "int", "bool", "datetime"]
)
ALL_PYSPARK_TYPES = Union[
    T.DataType,
    T.NullType,
    T.CharType,
    T.StringType,
    T.VarcharType,
    T.BinaryType,
    T.BooleanType,
    T.DateType,
    T.TimestampType,
    T.TimestampNTZType,
    T.DecimalType,
    T.DoubleType,
    T.FloatType,
    T.ByteType,
    T.IntegerType,
    T.LongType,
    T.DayTimeIntervalType,
    T.YearMonthIntervalType,
    T.ShortType,
    T.ArrayType,
    T.MapType,
    T.StructType,
]

VALID_PYAPARK_JOIN_TYPES = Literal[
    "inner",
    "cross",
    "outer",
    "full",
    "fullouter",
    "full_outer",
    "left",
    "leftouter",
    "left_outer",
    "right",
    "rightouter",
    "right_outer",
    "semi",
    "leftsemi",
    "left_semi",
    "anti",
    "leftanti",
    "left_anti",
]
ALL_PYSPARK_JOIN_TYPES = set(get_args(VALID_PYAPARK_JOIN_TYPES))


# ---------------------------------------------------------------------------- #
#  DataFrames                                                               ####
# ---------------------------------------------------------------------------- #


LITERAL_PANDAS_DATAFRAME_NAMES = Literal[
    "pandas.DataFrame",
    "pandas",
    "pd.DataFrame",
    "pd.df",
    "pddf",
    "pdDataFrame",
    "pdDF",
    "pd",
]

LITERAL_PYSPARK_DATAFRAME_NAMES = Literal[
    "spark.DataFrame",
    "pyspark.DataFrame",
    "pyspark",
    "spark",
    "ps.DataFrame",
    "ps.df",
    "psdf",
    "psDataFrame",
    "psDF",
    "ps",
]

LITERAL_NUMPY_ARRAY_NAMES = Literal[
    "numpy.array",
    "np.array",
    "np",
    "numpy",
    "nparr",
    "npa",
    "np.arr",
    "np.a",
]

LITERAL_LIST_OBJECT_NAMES = Literal["list", "lst", "l", "flat_list", "flatten_list"]

VALID_PANDAS_DATAFRAME_NAMES = set(get_args(LITERAL_PANDAS_DATAFRAME_NAMES))
VALID_PYSPARK_DATAFRAME_NAMES = set(get_args(LITERAL_PYSPARK_DATAFRAME_NAMES))
VALID_NUMPY_ARRAY_NAMES = set(get_args(LITERAL_NUMPY_ARRAY_NAMES))
VALID_LIST_OBJECT_NAMES = set(get_args(LITERAL_LIST_OBJECT_NAMES))

VALID_DATAFRAME_NAMES: str_set = VALID_PANDAS_DATAFRAME_NAMES.union(
    VALID_PYSPARK_DATAFRAME_NAMES
)


# ---------------------------------------------------------------------------- #
#  Other                                                                    ####
# ---------------------------------------------------------------------------- #


_DEFAULT_DEPRECATION_WARNING_CLASS = DeprecationWarning
_DEFAULT_DEPRECATION_WARNING = partial(warn, category=_DEFAULT_DEPRECATION_WARNING_CLASS)
