"""Port of QR.ipynb - three implementations of extent -> quad/row cell-key expansion.

Compares a numba-jitted @udf, a pure-numpy @udf and a numba+pandas @pandas_udf. The
notebook assumes pre-injected 'spark' and 'sql' globals; _harness.start() supplies both.

Note the bit-packing here (q << 32 | r) is exactly the sort of expression Spark 4's ANSI
mode would reject on overflow - it passes because spark_start defaults ansi off.
"""
import math
from typing import List

import numpy as np
import pandas as pd

from _harness import ROWS, check, report, require, start, stop  # first: puts python/ on sys.path

import spark_esri  # noqa: F401 - puts Pro's bundled pyspark on sys.path when it is not pip installed
from pyspark.sql.functions import col, explode, lit, pandas_udf, rand, udf
from pyspark.sql.types import ArrayType, LongType

numba = require("numba", "Run 'pip install numba' to enable this test.")
njit = numba.njit


@njit()
def qr_jit(xmin: float, ymin: float, xmax: float, ymax: float, cell: float) -> List[int]:
    qmin = math.floor(xmin / cell)
    qmax = math.floor(xmax / cell) + 1
    rmin = math.floor(ymin / cell)
    rmax = math.floor(ymax / cell) + 1
    return [(q << 32 | r & 0xFFFFFFFF) for q in range(qmin, qmax) for r in range(rmin, rmax)]


@udf(ArrayType(LongType()))
def qr(xmin: float, ymin: float, xmax: float, ymax: float, cell: float) -> List[int]:
    return qr_jit(xmin, ymin, xmax, ymax, cell)


@njit()
def _qr(row):
    xmin, ymin, xmax, ymax, cell = row
    qmin = math.floor(xmin / cell)
    qmax = math.floor(xmax / cell) + 1
    rmin = math.floor(ymin / cell)
    rmax = math.floor(ymax / cell) + 1
    return [(q << 32 | r & 0xFFFFFFFF) for q in range(qmin, qmax) for r in range(rmin, rmax)]


@pandas_udf(ArrayType(LongType()))
def qr_pd(xmin: pd.Series, ymin: pd.Series, xmax: pd.Series, ymax: pd.Series,
          cell: pd.Series) -> pd.Series:
    stack = np.dstack([xmin, ymin, xmax, ymax, cell]).reshape(-1, 5)
    return pd.Series(map(_qr, stack))


# NYC bounding box, as in the notebook.
xmin, ymin, xmax, ymax = (-74.2555913638106, 40.496115395209344,
                          -73.70000906387119, 40.91553277600007)
xdel, ydel = xmax - xmin, ymax - ymin
cell = 0.0002

spark, sql = start()
try:
    report("numba version", numba.__version__)
    report("rows", ROWS)

    df = (
        spark.range(ROWS)
        .withColumn("xmin", xmin + xdel * rand())
        .withColumn("ymin", ymin + ydel * rand())
        .withColumn("xmax", col("xmin") + 0.0001 * rand())
        .withColumn("ymax", col("ymin") + 0.0001 * rand())
        .withColumn("cell", lit(cell))
        .persist()
    )
    check("input rows", df.count(), ROWS)

    df.withColumn("qr", explode(qr("xmin", "ymin", "xmax", "ymax", "cell"))) \
        .createOrReplaceTempView("v0")
    max_pop = sql("select max(pop) max_pop from (select qr,count(qr) pop from v0 group by qr)") \
        .collect()[0].max_pop
    report("njit @udf max cell population", max_pop)
    assert max_pop and max_pop > 0

    df.withColumn("qr", explode(qr_pd("xmin", "ymin", "xmax", "ymax", "cell"))) \
        .createOrReplaceTempView("v1")
    check("pandas_udf agrees with udf on total keys",
          sql("select count(*) c from v1").collect()[0].c,
          sql("select count(*) c from v0").collect()[0].c)
finally:
    stop()

print("PASS")
