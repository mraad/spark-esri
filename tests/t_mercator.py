"""Port of MercatorUDF.ipynb - Mercator projection as a pandas UDF.

The notebook's GPU variants (cupy / numba.cuda cuLonToX) and the cuda.is_available() /
np.__config__.show() cells are dropped - there is no GPU in a headless smoke test, and the
notebook's cuLonToX references a 'cp' that its own import cell leaves commented out.
"""
import math

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from _harness import ROWS, check, report, start, stop


@pandas_udf(returnType=DoubleType())
def pdLonToX(lon: pd.Series) -> pd.Series:
    return 6378137.0 * np.radians(lon)


@pandas_udf(returnType=DoubleType())
def pdLatToY(lat: pd.Series) -> pd.Series:
    return 6378137.0 * np.log(np.tan((math.pi * 0.25) + (0.5 * np.radians(lat))))


spark, sql = start()
try:
    report("numpy version", np.__version__)
    report("rows", ROWS)

    (
        spark.range(ROWS)
        .selectExpr("id",
                    "-180.0+360.0*rand() lon1", "-90.0+180.0*rand() lat1",
                    "-180.0+360.0*rand() lon2", "-90.0+180.0*rand() lat2")
        .withColumn("x", pdLonToX("lon1"))
        .withColumn("y", pdLatToY("lat1"))
        .createOrReplaceTempView("v0")
    )
    row = sql("select avg(x) avg_x, min(x) min_x, max(x) max_x from v0").collect()[0]
    report("avg x", row.avg_x)

    half = 6378137.0 * math.pi
    assert row.min_x >= -half - 1e-6 and row.max_x <= half + 1e-6, \
        f"x outside the Mercator extent: [{row.min_x}, {row.max_x}]"
    check("no null x", sql("select count(*) c from v0 where x is null").collect()[0].c, 0)

    # Known fixed points.
    known = spark.sql("select 0.0D z, 180.0D lon, 0.0D lat") \
        .withColumn("x", pdLonToX("lon")).withColumn("y", pdLatToY("lat")).collect()[0]
    report("lonToX(180)", known.x)
    assert abs(known.x - half) < 1e-6, f"lonToX(180) should be {half}, got {known.x}"
    assert abs(known.y) < 1e-9, f"latToY(0) should be 0, got {known.y}"
finally:
    stop()

print("PASS")
