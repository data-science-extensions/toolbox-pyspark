# ## Python Third Party Imports ----
from pyspark import __version__ as version

# ## Local First Party Imports ----
from toolbox_pyspark.utils.exceptions import PySparkVersionError


class PySparkVersion:
    def __init__(self, version: str) -> None:
        self.major, self.minor, self.patch = (int(elem) for elem in version.split("."))


ver = PySparkVersion(version)


if ver.major < 3 or (ver.major == 3 and ver.minor < 3):
    raise PySparkVersionError(
        "PySpark version >= `3.3.0` is required to use the `.transform()` method."
        "For more info, see: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html"
    )
