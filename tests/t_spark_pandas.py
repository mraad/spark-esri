"""Port of SparkPandas.ipynb - numba-accelerated haversine as a pandas UDF.

The notebook's cuda.is_available() cell is dropped (no GPU in a headless smoke test) and
the row count is scaled down from 10M. The notebook also assumes pre-injected 'spark' and
'sql' globals; _harness.start() supplies both.
"""
import math

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from _harness import ROWS, check, report, require, start, stop

numba = require("numba", "Run 'pip install numba' to enable this test.")
jit = numba.jit


@jit(nopython=True)
def _haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6378137.0 * 2.0 * np.arcsin(np.sqrt(a))


spark, sql = start()
try:
    # These must be declared AFTER the session exists: @pandas_udf("double") passes a DDL
    # *string*, and Spark parses it through the JVM, so decorating at module scope raises
    # [SESSION_OR_CONTEXT_NOT_EXISTS]. In a notebook 'spark' is already live by this point.
    # (@pandas_udf(DoubleType()) takes a python object and has no such ordering constraint.)
    @pandas_udf(returnType=DoubleType())
    def lonToX(lon: pd.Series) -> pd.Series:
        return 6378137.0 * np.radians(lon)

    @pandas_udf(returnType=DoubleType())
    def latToY(lat: pd.Series) -> pd.Series:
        return 6378137.0 * np.log(np.tan((math.pi * 0.25) + (0.5 * np.radians(lat))))

    @pandas_udf("double")
    def haversine(lon1: pd.Series, lat1: pd.Series,
                  lon2: pd.Series, lat2: pd.Series) -> pd.Series:
        # Convert pandas series to numpy array for numba
        return pd.Series(_haversine(lon1.values, lat1.values, lon2.values, lat2.values))

    report("numba version", numba.__version__)
    report("rows", ROWS)

    (
        spark.range(ROWS)
        .selectExpr("id",
                    "-180.0+360.0*rand() lon1", "-90.0+180.0*rand() lat1",
                    "-180.0+360.0*rand() lon2", "-90.0+180.0*rand() lat2")
        .withColumn("meters", haversine("lon1", "lat1", "lon2", "lat2"))
        .createOrReplaceTempView("v0")
    )
    row = sql("select avg(meters) avg_meters, min(meters) min_m, max(meters) max_m from v0").collect()[0]
    report("avg meters", row.avg_meters)

    # Great-circle distance on a sphere of radius R is bounded by pi*R (~20015 km).
    assert row.min_m >= 0.0, f"negative distance: {row.min_m}"
    assert row.max_m <= math.pi * 6378137.0 + 1.0, f"distance exceeds half circumference: {row.max_m}"
    check("no null distances", sql("select count(*) c from v0 where meters is null").collect()[0].c, 0)

    # Mercator UDFs: a known fixed point - lon 180 maps to +half the equator.
    x = spark.sql("select 180.0D lon").withColumn("x", lonToX("lon")).collect()[0].x
    report("lonToX(180)", x)
    assert abs(x - 6378137.0 * math.pi) < 1e-6
    y = spark.sql("select 0.0D lat").withColumn("y", latToY("lat")).collect()[0].y
    assert abs(y) < 1e-9, f"latToY(0) should be 0, got {y}"
finally:
    stop()

print("PASS")
