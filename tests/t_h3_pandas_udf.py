"""Port of H3_Pandas_UDF.ipynb - h3 indexing through a pandas UDF over synthetic points.

Ported from the h3 v3 API used by the notebook to the v4 API:
    h3.geo_to_h3(lat, lon, res)  ->  h3.latlng_to_cell(lat, lng, res)
"""
import pandas as pd
from pyspark.sql.functions import avg, pandas_udf
from pyspark.sql.types import StringType

from _harness import ROWS, check, report, require, start, stop

h3 = require("h3", "Run 'pip install h3' to enable this test.")

# h3 v4 renamed nearly every function; fail loudly rather than silently skipping if the
# installed major version is one we have not ported to.
if not hasattr(h3, "latlng_to_cell"):
    raise AssertionError(
        f"h3 {getattr(h3, '__version__', '?')} has no latlng_to_cell - this test targets "
        f"the h3 v4 API (v3 called it geo_to_h3)."
    )

RESOLUTION = 9


@pandas_udf(returnType=StringType())
def geo_to_h3(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return pd.Series([h3.latlng_to_cell(a, b, RESOLUTION) for a, b in zip(lat, lon)])


spark, sql = start()
try:
    report("h3 version", getattr(h3, "__version__", "?"))
    report("rows", ROWS)

    df = (
        spark.range(ROWS)
        .selectExpr("id", "rand()*360D-180D lon", "rand()*180D-90D lat")
        .withColumn("h3", geo_to_h3("lat", "lon"))
    )
    agg = df.groupby("h3").agg(avg("lon").alias("a_lon"), avg("lat").alias("a_lat"))
    agg.show(5)

    check("no null h3 cells", df.filter("h3 is null").count(), 0)

    cells = df.select("h3").distinct().count()
    report("distinct h3 cells", cells)
    assert cells > 1, "all points hashed to a single cell - the UDF is not doing anything"

    # Round-trip one cell through the v4 API to prove it is a real index, not a string.
    sample = df.select("h3").first().h3
    report("sample cell", sample)
    check("cell resolution", h3.get_resolution(sample), RESOLUTION)
    boundary = h3.cell_to_boundary(sample)
    assert len(boundary) >= 5, f"unexpected hex boundary: {boundary}"
finally:
    stop()

print("PASS")
