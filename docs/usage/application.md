# Application

The application of functions in this package is quite versatile and flexible. You can adapt it to suit any of your needs.

## Example Pipelines

Here's a super simple pipeline for us to use show how to use it.

In the below example, we will use some data from PyCaret regarding Association Rule Mining. For more info, see here: https://github.com/pycaret/pycaret/blob/master/datasets/index.csv

After we import our packages and instantiate spark, we will do the following:

1. Load the data from the source URL using the Pandas API using the [`pd.read_csv()`][.read_csv] method.
1. Convert that Pandas DataFrame to a PySpark DataFrame using the [`spark.createDataFrame()`][.createDataFrame] method.
1. Convert the relevant columns to their correct type using the [`.transform()`][.transform] method with the [`map_cast_columns_to_type`][toolbox_pyspark.types.map_cast_columns_to_type] function.
1. Convert the `InvoiceDate` column to a `timestamp` format in a specific format by using the [`.withColumn()`][.withColumn] method and the [`to_timestamp()`][to_timestamp].
1. Rename all columns to convert them to uppercase using the [`.transform()`][.transform] method with the [`rename_columns`][toolbox_pyspark.columns.rename_columns] function.
1. Rename the `INVOICEDATE` column to `INVOICEDATETIME` using the [`.transform()`][.transform] method with the [`rename_datetime_columns()`][toolbox_pyspark.datetime.rename_datetime_columns] function.
1. Split the `INVOICEDATETIME` column in to two different columns `INVOICEDATE` and `INVOICETIME` using the [`.transform()`][.transform] method with the [`split_datetime_columns()`][toolbox_pyspark.datetime.split_datetime_columns] function.

The beauty of this process is in the fact that everything remains in the same chain of methods, so it is incredibly smooth and streamlined to implement, plus it is computationally efficient.

```{.py .python linenums="1" title="Simple Example"}
# Import packages
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from toolbox_pyspark import (
    map_cast_columns_to_type,
    split_datetime_columns,
    rename_datetime_columns,
    rename_columns,
)

# Create spark
spark = SparkSession.builder.getOrCreate()

# Data source
url = "https://raw.githubusercontent.com/pycaret/pycaret/master/datasets/germany.csv"

(
    spark.createDataFrame(pd.read_csv(url))
    .transform(
        map_cast_columns_to_type, #(1)
        columns_type_mapping={
            "string": [
                "InvoiceNo",
                "StockCode",
                "Description",
                "Country",
                "CustomerID",
                "InvoiceDate",
            ],
            "int": ["Quantity"],
            "float": ["UnitPrice"],
        },
    )
    .withColumn(
        "InvoiceDate",
        F.to_timestamp("InvoiceDate", "MM/d/yyyy H:mm"),
    )
    .transform(
        rename_columns, #(2)
        string_function="upper",
    )
    .transform(
        rename_datetime_columns, #(3)
        columns="INVOICEDATE",
    )
    .transform(
        split_datetime_columns, #(4)
        columns="INVOICEDATETIME",
    )
    .show()
)
```

1. This function will cast lists of columns to their respective data types. For more info, see: [`map_cast_columns_to_type`][toolbox_pyspark.types.map_cast_columns_to_type].
2. This function to renames all columns to uppercase. For more info, see: [`rename_columns`][toolbox_pyspark.columns.rename_columns].
3. This function renames all columns that have the type `timestamp` to append `TIME` on the end of the name. For more info, see: [`rename_datetime_columns`][toolbox_pyspark.datetime.rename_datetime_columns].
4. This function takes all columns with the type `timestamp` and for each of them, will add two new columns containing the `date` component and the `time` component. For more info, see: [`split_datetime_columns`][toolbox_pyspark.datetime.split_datetime_columns].

Which will give you the result:

```txt
+---------+---------+--------------------+--------+-------------------+---------+----------+-------+-----------+-----------+
|INVOICENO|STOCKCODE|         DESCRIPTION|QUANTITY|    INVOICEDATETIME|UNITPRICE|CUSTOMERID|COUNTRY|INVOICEDATE|INVOICETIME|
+---------+---------+--------------------+--------+-------------------+---------+----------+-------+-----------+-----------+
|   536527|    22809|SET OF 6 T-LIGHTS...|       6|2010-12-01 13:04:00|     2.95|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    84347|ROTATING SILVER A...|       6|2010-12-01 13:04:00|     2.55|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    84945|MULTI COLOUR SILV...|      12|2010-12-01 13:04:00|     0.85|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22242|5 HOOK HANGER MAG...|      12|2010-12-01 13:04:00|     1.65|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22244|3 HOOK HANGER MAG...|      12|2010-12-01 13:04:00|     1.95|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22243|5 HOOK HANGER RED...|      12|2010-12-01 13:04:00|     1.65|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    47421|ASSORTED COLOUR L...|      24|2010-12-01 13:04:00|     0.42|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    20712|JUMBO BAG WOODLAN...|      10|2010-12-01 13:04:00|     1.95|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    20713|      JUMBO BAG OWLS|      10|2010-12-01 13:04:00|     1.95|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22837|HOT WATER BOTTLE ...|       4|2010-12-01 13:04:00|     4.65|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22969|HOMEMADE JAM SCEN...|      12|2010-12-01 13:04:00|     1.45|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22973|CHILDREN'S CIRCUS...|      12|2010-12-01 13:04:00|     1.65|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|   84569B|PACK 3 FIRE ENGIN...|      12|2010-12-01 13:04:00|     1.25|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|    22549|    PICTURE DOMINOES|      12|2010-12-01 13:04:00|     1.45|     12662|Germany| 2010-12-01|   13:04:00|
|   536527|     POST|             POSTAGE|       1|2010-12-01 13:04:00|     18.0|     12662|Germany| 2010-12-01|   13:04:00|
|  C536548|    22244|3 HOOK HANGER MAG...|      -4|2010-12-01 14:33:00|     1.95|     12472|Germany| 2010-12-01|   14:33:00|
|  C536548|    22242|5 HOOK HANGER MAG...|      -5|2010-12-01 14:33:00|     1.65|     12472|Germany| 2010-12-01|   14:33:00|
|  C536548|    20914|SET/5 RED RETROSP...|      -1|2010-12-01 14:33:00|     2.95|     12472|Germany| 2010-12-01|   14:33:00|
|  C536548|    22892|SET OF SALT AND P...|      -7|2010-12-01 14:33:00|     1.25|     12472|Germany| 2010-12-01|   14:33:00|
|  C536548|    22654|  DELUXE SEWING KIT |      -1|2010-12-01 14:33:00|     5.95|     12472|Germany| 2010-12-01|   14:33:00|
+---------+---------+--------------------+--------+-------------------+---------+----------+-------+-----------+-----------+
```

This process can then be continued to be implemented over larger tables, and longer and more complex pipelines.

## FAQ

<div class="grid cards" markdown>


- **:material-numeric-1-circle-outline: Why design a functional-oriented package, and not use object-oriented methodology? :material-function-variant:**

    ---

    Because of how the PySpark API is set up. More specifically, in the use of the [`.transform()`][.transform] method. This requires a single callable object, which returns a PySpark dataframe from it. The [`.transform()`][.transform] method is quite powerful and versatile for our purposes.

- **:material-numeric-2-circle-outline: What version of PySpark is required to use the full features of this package? :1234:**

    ---

    PySpark version `3.3.0` or higher is required. This is because the [`.transform()`][.transform] method had the `*args` and `**kwargs` parameters added in version `3.3.0`. If you try to use a version earlier than this, then a [`PySparkVersionError`][toolbox_pyspark.utils.exceptions.PySparkVersionError] will be raised during initialisation of this package.

- **:material-numeric-3-circle-outline: Why shouldn't I just use the default PySpark functions out-of-the-box? :package:**

    ---

    PySpark functions are designed to be generic, single-use functions. Which is fine, and it works well. This [`toolbox-pyspark`][toolbox-pyspark] package is designed to leverage the PySpark base functionality, and add more conveniences and extensions to it. It also reduces hundreds of lines of pure PySpark code in to just a few convenient lines, if you make full use of the [`.transform()`][.transform] method.

</div>

[.read_csv]: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
[to_timestamp]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html
[.createDataFrame]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html
[.withColumn]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
[.transform]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html
[toolbox-pyspark]: https://data-science-extensions.com/toolbox-pyspark/
