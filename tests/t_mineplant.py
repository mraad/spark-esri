"""Port of mineplant.ipynb - CSV read with an explicit schema, toPandas, Parquet write.

The only notebook backed by data that lives in the repo (mineplant_2.txt). The notebook
hard codes 'Z:\\GWorkspace\\spark_esri\\mineplant_2.txt' and writes to 'C:\\TEMP'; both are
resolved relative to the checkout / a temp dir here so the test is portable.
"""
import os
import tempfile

from _harness import check, report, repo_root, start, stop  # first: puts python/ on sys.path

import spark_esri  # noqa: F401 - puts Pro's bundled pyspark on sys.path when it is not pip installed
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

schema = StructType(
    [
        StructField("ID", IntegerType()),
        StructField("COMMODITY", StringType()),
        StructField("SITE_NAME", StringType()),
        StructField("COMPANY_NA", StringType()),
        StructField("STATE_LOCA", StringType()),
        StructField("COUNTY", StringType()),
        StructField("LATITUDE", DoubleType()),
        StructField("LONGITUDE", DoubleType()),
        StructField("PLANT_MIN", StringType()),
    ]
)

path = os.path.join(repo_root(), "mineplant_2.txt")
assert os.path.isfile(path), f"missing repo data file: {path}"

spark, sql = start({"spark.sql.execution.arrow.pyspark.enabled": True})
try:
    mines = spark.read.csv(path=path, schema=schema, sep="\t", header=True)
    mines.printSchema()
    mines.show(3)

    count = mines.count()
    report("row count", count)
    assert count > 0, "mineplant_2.txt parsed to zero rows"
    check("column count", len(mines.columns), 9)

    # Arrow path - the driver side conversion that pandas 3.0 could plausibly break.
    pdf = mines.toPandas()
    check("toPandas row count", len(pdf), count)
    check("toPandas column count", len(pdf.columns), 9)

    # A schema'd numeric column must actually parse as a number, not silently go null.
    non_null_lat = mines.filter("LATITUDE is not null").count()
    report("non-null LATITUDE", non_null_lat)
    assert non_null_lat > 0, "LATITUDE parsed entirely to null - schema/separator mismatch"

    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "mines.prq")
        mines.write.parquet(out, mode="overwrite")
        check("parquet round-trip count", spark.read.parquet(out).count(), count)
finally:
    stop()

print("PASS")
