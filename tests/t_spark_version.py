"""Canary + SPARK-53759 regression test.  Port of spark_version.ipynb.

This is the test that matters most after a Pro or Spark upgrade: it proves executor side
python actually runs. ArcGIS Pro 3.7.1 ships Spark 4.1.1, which is affected by SPARK-53759
("missing flush in the simple-worker path") - on Windows with Python 3.12+ every @udf,
@pandas_udf and rdd.map() dies with 'Python worker exited unexpectedly (crashed)' /
[WinError 10038]. Fixed in Spark 3.5.9 / 4.0.3 / 4.1.2.
"""
import sys

from _harness import check, report, start, stop

spark, sql = start()
try:
    import spark_esri
    from spark_esri import _needs_spark53759_fix, _version_tuple

    report("SPARK_HOME", spark_esri.spark_home)
    report("spark.version", spark.version)
    report("pyspark.__version__", spark_esri.pyspark.__version__)

    # --- the version predicate itself, so the guard cannot silently rot -----------------
    check("_version_tuple('4.1.3')", _version_tuple("4.1.3"), (4, 1, 3))
    check("_version_tuple('4.1.2.dev1')", _version_tuple("4.1.2.dev1"), (4, 1, 2))
    for broken in ("3.5.5", "4.0.0", "4.0.1", "4.1.0", "4.1.1"):
        check(f"_needs_spark53759_fix({broken!r})", _needs_spark53759_fix(broken), True)
    for fixed in ("3.5.9", "4.0.3", "4.1.2", "4.1.3", "4.2.0"):
        check(f"_needs_spark53759_fix({fixed!r})", _needs_spark53759_fix(fixed), False)

    # --- driver side: works even on an affected Spark -----------------------------------
    check("range().count()", spark.range(1000).count(), 1000)
    check("toPandas().shape", spark.range(3).toPandas().shape, (3, 1))

    # --- ANSI compat: Spark 4 defaults this on, spark_start turns it back off ------------
    check("spark.sql.ansi.enabled", spark.conf.get("spark.sql.ansi.enabled"), "false")
    check("select 1/0", spark.sql("select 1/0 as x").collect()[0].x, None)

    # --- executor side: THIS is the SPARK-53759 regression test -------------------------
    check("rdd.map()", spark.range(3).rdd.map(lambda row: row.id * 2).collect(), [0, 2, 4])

    from pyspark.sql.functions import pandas_udf, udf
    from pyspark.sql.types import StringType
    import pandas as pd

    @pandas_udf("double")
    def double_it(s: pd.Series) -> pd.Series:
        return s * 2.0

    check("@pandas_udf", [r.d for r in spark.range(3).select(double_it("id").alias("d")).collect()],
          [0.0, 2.0, 4.0])

    @udf(StringType())
    def tag(i):
        return f"row-{i}"

    check("@udf", [r.t for r in spark.range(2).select(tag("id").alias("t")).collect()],
          ["row-0", "row-1"])

    if _needs_spark53759_fix(spark_esri.pyspark.__version__):
        print("UNEXPECTED: the UDFs above passed on a Spark that should be affected by "
              "SPARK-53759 - the version predicate may need revisiting.", file=sys.stderr)
finally:
    stop()

print("PASS")
