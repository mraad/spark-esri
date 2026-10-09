"""End to end on real data - the NorthSea project geodatabase, read only.

Reads the Wellbores (point), Pipelines (polyline) and Discoveries (polygon) feature classes
through arcpy, pushes them through Spark (SQL binning, an executor side pandas UDF, group
by) and writes the results back with every insert_cursor entry point. Each Spark result is
checked against the same quantity computed independently with plain arcpy / python.

Nothing is written to the source geodatabase - all output goes to the "memory" workspace.
SKIPs when the geodatabase is absent; point SPARK_ESRI_NORTHSEA_GDB at another copy.
"""
import math
import os
from collections import Counter

import arcpy
import pandas as pd

from _harness import check, report, skip, start, stop  # first: puts python/ on sys.path

import spark_esri  # noqa: F401 - puts Pro's bundled pyspark on sys.path when it is not pip installed
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (BinaryType, DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

import insert_cursor as ic

GDB = os.environ.get("SPARK_ESRI_NORTHSEA_GDB",
                     r"C:\Mac\Home\Documents\ArcGIS\Projects\NorthSea\NorthSea.gdb")
if not arcpy.Exists(GDB):
    skip(f"'{GDB}' not found - set SPARK_ESRI_NORTHSEA_GDB to a copy of NorthSea.gdb.")

CELL = 0.25  # bin size in degrees
ABERDEEN = (57.1497, -2.0943)  # lat, lon


def source(name):
    return os.path.join(GDB, name)


def search(path, fields):
    # Always under `with` - an unreleased SearchCursor holds a schema lock.
    with arcpy.da.SearchCursor(path, fields) as cur:
        return [tuple(r) for r in cur]


def count(name):
    return int(arcpy.management.GetCount(f"memory/{name}")[0])


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(a))


@pandas_udf(DoubleType())  # a DDL string return type would need a live session first
def km_to_aberdeen(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return pd.Series([haversine_km(a, b, *ABERDEEN) for a, b in zip(lat, lon)])


OUTPUTS = ("ns_bins", "ns_wells", "ns_discoveries", "ns_pipelines")

spark, sql = start()
try:
    sr = arcpy.Describe(source("Wellbores")).spatialReference.factoryCode
    report("arcpy version", arcpy.GetInstallInfo()["Version"])
    report("spark version", spark.version)
    report("source", GDB)
    report("spatial reference", sr)

    # ------------------------------------------------------------ Wellbores -> DataFrame
    wells = search(source("Wellbores"),
                   ["SHAPE@X", "SHAPE@Y", "OID@", "purpose", "water_depth", "entry_date"])
    wells = [w for w in wells if w[0] is not None and w[1] is not None]
    report("wellbores", len(wells))
    assert len(wells) > 1000, "Wellbores is unexpectedly small"
    # Via pandas (Arrow), not a list of tuples: the row path converts datetimes with
    # time.mktime(), which rejects pre-1970 dates on Windows - and NorthSea has 1960s wells.
    # Keyed on OBJECTID throughout - wellbore_name is NOT unique in this data.
    wells_pd = pd.DataFrame(wells, columns=["x", "y", "src_oid", "purpose",
                                            "water_depth", "entry_date"])
    wells_pd["entry_date"] = pd.to_datetime(wells_pd["entry_date"])
    report("oldest entry_date", min(w[5] for w in wells if w[5] is not None))
    df = spark.createDataFrame(wells_pd, schema=StructType([
        StructField("x", DoubleType()), StructField("y", DoubleType()),
        StructField("src_oid", LongType()), StructField("purpose", StringType()),
        StructField("water_depth", DoubleType()), StructField("entry_date", TimestampType()),
    ]))
    df.createOrReplaceTempView("wells")
    check("spark row count == arcpy row count", df.count(), len(wells))

    # ------------------------------------------------------------ spatial binning (SQL)
    bins = sql(f"""
        select concat('POLYGON((', x0, ' ', y0, ',', x0, ' ', y1, ',', x1, ' ', y1, ',',
                      x1, ' ', y0, ',', x0, ' ', y0, '))') wkt, q, r, n, depth
        from (select q, r, n, depth,
                     q * {CELL} x0, (q + 1) * {CELL} x1, r * {CELL} y0, (r + 1) * {CELL} y1
              from (select cast(floor(x / {CELL}) as int) q, cast(floor(y / {CELL}) as int) r,
                           count(1) n, avg(water_depth) depth
                    from wells group by 1, 2))
    """)
    ic.insert_df(bins, "ns_bins", spatial_reference=sr, shape_format="WKT")
    expected_bins = Counter((math.floor(x / CELL), math.floor(y / CELL)) for x, y, *_ in wells)
    with arcpy.da.SearchCursor("memory/ns_bins", ["q", "r", "n", "SHAPE@AREA"]) as cur:
        actual_bins = {}
        areas = []
        for q, r, n, area in cur:
            actual_bins[(q, r)] = n
            areas.append(area)
    check("bin feature count", count("ns_bins"), len(expected_bins))
    bad_bins = [k for k in expected_bins.keys() | actual_bins.keys()
                if expected_bins.get(k) != actual_bins.get(k)]
    check("bins disagreeing with a pure python binning", bad_bins, [])
    check("wells across all bins", sum(actual_bins.values()), len(wells))
    assert all(abs(a - CELL * CELL) < 1e-9 for a in areas), "bin polygons are not CELL x CELL"

    # ------------------------------------------------------------ executor side pandas UDF
    with_km = df.withColumn("km", km_to_aberdeen("y", "x"))
    by_oid = {w[2]: haversine_km(w[1], w[0], *ABERDEEN) for w in wells}
    worst = max(abs(row.km - by_oid[row.src_oid])
                for row in with_km.select("src_oid", "km").toLocalIterator())
    report("max |udf - python| km", worst)
    assert worst < 1e-6, f"pandas UDF disagrees with python by {worst} km"

    # ------------------------------------------------------------ insert_df_xy + dates
    ic.insert_df_xy(with_km.select("x", "y", "src_oid", "water_depth", "entry_date", "km"),
                    "ns_wells", spatial_reference=sr)
    check("well feature count", count("ns_wells"), len(wells))
    back = {r[0]: r[1:] for r in search("memory/ns_wells",
                                        ["src_oid", "entry_date", "water_depth", "km"])}
    check("every source OBJECTID came back once", len(back), len(wells))
    mismatched_dates = [w[2] for w in wells if back[w[2]][0] != w[5]]
    check("entry_date survives arcpy -> spark -> arcpy", mismatched_dates[:5], [])
    mismatched_depth = [w[2] for w in wells if back[w[2]][1] != w[4]]
    check("water_depth survives the round trip", mismatched_depth[:5], [])
    km_out = max(abs(back[n][2] - km) for n, km in by_oid.items())
    assert km_out < 1e-6, f"km attribute drifted by {km_out}"

    # ------------------------------------------------------------ Discoveries (WKB polygons)
    discoveries = [(bytes(wkb), hc, area)
                   for wkb, hc, area in search(source("Discoveries"),
                                               ["SHAPE@WKB", "discovery_hc_type", "SHAPE@AREA"])
                   if wkb is not None]
    report("discoveries", len(discoveries))
    ddf = spark.createDataFrame([d[:2] for d in discoveries], schema=StructType([
        StructField("shape", BinaryType()), StructField("hc_type", StringType())]))
    hc_counts = {r.hc_type: r.n for r in ddf.groupBy("hc_type").count()
                 .withColumnRenamed("count", "n").collect()}
    check("group by hc_type matches python", hc_counts,
          dict(Counter(d[1] for d in discoveries)))
    ic.insert_df(ddf, "ns_discoveries", spatial_reference=sr)
    check("discovery feature count", count("ns_discoveries"), len(discoveries))
    area_in = sum(d[2] for d in discoveries)
    area_out = sum(r[0] for r in search("memory/ns_discoveries", ["SHAPE@AREA"]))
    report("total discovery area (sq deg)", area_out)
    assert abs(area_out - area_in) <= 1e-6 * area_in, f"area {area_in} -> {area_out}"

    # ------------------------------------------------------------ Pipelines (WKB polylines)
    pipes = [(bytes(wkb), medium, length)
             for wkb, medium, length in search(source("Pipelines"),
                                               ["SHAPE@WKB", "medium", "SHAPE@LENGTH"])
             if wkb is not None]
    report("pipelines", len(pipes))
    pdf = spark.createDataFrame([p[:2] for p in pipes], schema=StructType([
        StructField("shape", BinaryType()), StructField("medium", StringType())]))
    ic.insert_df(pdf, "ns_pipelines", spatial_reference=sr, shape_type="POLYLINE")
    check("pipeline feature count", count("ns_pipelines"), len(pipes))
    len_in = sum(p[2] for p in pipes)
    len_out = sum(r[0] for r in search("memory/ns_pipelines", ["SHAPE@LENGTH"]))
    assert abs(len_out - len_in) <= 1e-6 * len_in, f"length {len_in} -> {len_out}"
    report("total pipeline length (deg)", len_out)
finally:
    for name in OUTPUTS:
        arcpy.management.Delete(f"memory/{name}")
    stop()

print("PASS")
