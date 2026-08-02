"""Headless tests for python/insert_cursor - no open ArcGIS Pro project needed.

These functions only touch arcpy.management.CreateFeatureclass and arcpy.da.InsertCursor
against the in-memory workspace, all of which work in standalone arcpy. What genuinely
requires a live Pro project is arcpy.mp and named map layers, which insert_cursor never
uses - so the whole module is testable here, including the three functions that have no
callers anywhere in the repo (insert_df_xy, insert_df_hex, insert_df_progress).

Every feature class is written to the "memory" workspace and deleted afterwards.
"""
import datetime
import decimal

import arcpy
from pyspark.sql.types import (BinaryType, BooleanType, DateType, DecimalType, DoubleType,
                               FloatType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

from _harness import check, report, start, stop

import insert_cursor as ic

SR = 3857
sr = arcpy.SpatialReference(SR)


def square(x, y, size=10.0):
    """A closed square polygon, as arcpy geometry."""
    return arcpy.Polygon(
        arcpy.Array([arcpy.Point(x, y), arcpy.Point(x, y + size),
                     arcpy.Point(x + size, y + size), arcpy.Point(x + size, y)]), sr)


def fc_rows(name, fields):
    # Always under `with` - an unreleased SearchCursor holds a schema lock and makes the
    # later arcpy.management.Delete in cleanup() fail intermittently.
    with arcpy.da.SearchCursor(f"memory/{name}", fields) as cur:
        return [tuple(r) for r in cur]


def count(name):
    return int(arcpy.management.GetCount(f"memory/{name}")[0])


def cleanup(*names):
    for n in names:
        arcpy.management.Delete(f"memory/{n}")


spark, sql = start()
try:
    report("arcpy version", arcpy.GetInstallInfo()["Version"])
    report("license", arcpy.ProductInfo())

    # ---------------------------------------------------------------- _df_to_fields
    # The Spark type -> Esri field type mapping, including the leading-columns offset.
    typed = spark.createDataFrame(
        [(b"", 1, 2, 3.0, 4.0, decimal.Decimal("5.0"), datetime.date(2020, 1, 1),
          datetime.datetime(2020, 1, 1, 12, 0), "s", True)],
        schema=StructType([
            StructField("shape", BinaryType()), StructField("i", IntegerType()),
            StructField("l", LongType()), StructField("f", FloatType()),
            StructField("d", DoubleType()), StructField("dec", DecimalType(10, 2)),
            StructField("date", DateType()), StructField("ts", TimestampType()),
            StructField("s", StringType()), StructField("b", BooleanType()),
        ]))
    check("_df_to_fields skips leading shape column",
          ic._df_to_fields(typed, 1),
          [("i", "LONG"), ("l", "LONG"), ("f", "DOUBLE"), ("d", "DOUBLE"),
           ("dec", "DOUBLE"), ("date", "DATE"), ("ts", "DATE"), ("s", "STRING"),
           ("b", "STRING")])
    check("_df_to_fields honours an offset of 2",
          [n for n, _ in ic._df_to_fields(typed, 2)],
          ["l", "f", "d", "dec", "date", "ts", "s", "b"])

    # ---------------------------------------------------------------- insert_df (WKB)
    polys = spark.createDataFrame(
        [(bytes(square(0, 0).WKB), 10, "alpha"),
         (bytes(square(100, 100).WKB), 20, "beta")],
        schema=StructType([StructField("shape", BinaryType()),
                           StructField("pop", LongType()),
                           StructField("label", StringType())]))
    ic.insert_df(polys, "TestBins", ws="memory", spatial_reference=SR, shape_type="POLYGON")
    check("insert_df row count", count("TestBins"), 2)
    check("insert_df attributes round-trip",
          sorted(fc_rows("TestBins", ["pop", "label"])),
          [(10, "alpha"), (20, "beta")])
    desc = arcpy.Describe("memory/TestBins")
    check("insert_df shape type", desc.shapeType, "Polygon")
    check("insert_df spatial reference", desc.spatialReference.factoryCode, SR)
    with arcpy.da.SearchCursor("memory/TestBins", ["SHAPE@"]) as cur:
        areas = sorted(round(r[0].area) for r in cur)
    check("insert_df geometry survived (two 10x10 squares)", areas, [100, 100])

    # ---------------------------------------------------------------- insert_df (WKT)
    wkt = spark.createDataFrame(
        [(square(0, 0).WKT, 1)],
        schema=StructType([StructField("shape", StringType()),
                           StructField("pop", LongType())]))
    ic.insert_df(wkt, "TestWkt", ws="memory", spatial_reference=SR,
                 shape_type="POLYGON", shape_format="WKT")
    check("insert_df WKT row count", count("TestWkt"), 1)
    with arcpy.da.SearchCursor("memory/TestWkt", ["SHAPE@"]) as cur:
        wkt_area = round(next(iter(cur))[0].area)
    check("insert_df WKT geometry area", wkt_area, 100)

    # ---------------------------------------------------------------- insert_df_xy
    pts = spark.createDataFrame(
        [(0.0, 0.0, "origin"), (100.5, -200.25, "somewhere")],
        schema=StructType([StructField("x", DoubleType()), StructField("y", DoubleType()),
                           StructField("name", StringType())]))
    ic.insert_df_xy(pts, "TestPoints", ws="memory", spatial_reference=SR)
    check("insert_df_xy row count", count("TestPoints"), 2)
    check("insert_df_xy shape type", arcpy.Describe("memory/TestPoints").shapeType, "Point")
    with arcpy.da.SearchCursor("memory/TestPoints", ["SHAPE@X", "SHAPE@Y", "name"]) as cur:
        coords = sorted((round(r[0], 2), round(r[1], 2), r[2]) for r in cur)
    check("insert_df_xy coordinates round-trip", coords,
          [(0.0, 0.0, "origin"), (100.5, -200.25, "somewhere")])

    # ---------------------------------------------------------------- insert_df_progress
    # Same contract as insert_df, but drives Pro's progress bar and returns the fc path
    # (or None if the user cancelled). The progressor APIs are no-ops outside a GP tool,
    # so this runs headless just fine.
    # It sets arcpy.env.autoCancelling=False while it polls arcpy.env.isCancelled itself,
    # so assert it RESTORES the caller's value - asserting False here would instead pin the
    # leak this used to have, and would invert the moment the leak was fixed.
    arcpy.env.autoCancelling = True
    out = ic.insert_df_progress(polys, "TestProgress", ws="memory",
                                spatial_reference=SR, shape_type="POLYGON")
    check("insert_df_progress returns the fc path", out, "memory\\TestProgress")
    check("insert_df_progress row count", count("TestProgress"), 2)
    check("insert_df_progress attributes",
          sorted(fc_rows("TestProgress", ["pop", "label"])),
          [(10, "alpha"), (20, "beta")])
    check("insert_df_progress restores autoCancelling=True", arcpy.env.autoCancelling, True)
    arcpy.env.autoCancelling = False
    ic.insert_df_progress(polys, "TestProgress", ws="memory",
                          spatial_reference=SR, shape_type="POLYGON")
    check("insert_df_progress restores autoCancelling=False", arcpy.env.autoCancelling, False)

    # ---------------------------------------------------------------- low-level cursors
    fields = [("pop", "LONG"), ("label", "STRING")]
    with ic.insert_cursor("TestRaw", list(fields), ws="memory",
                          spatial_reference=SR, shape_type="POLYGON") as cur:
        cur.insertRow((square(0, 0).WKB, 7, "raw"))
    check("insert_cursor row count", count("TestRaw"), 1)
    check("insert_cursor attributes", fc_rows("TestRaw", ["pop", "label"]), [(7, "raw")])

    ic.insert_rows([(square(0, 0).WKB, 1, "a"), (square(20, 20).WKB, 2, "b")],
                   "TestRows", list(fields), ws="memory",
                   spatial_reference=SR, shape_type="POLYGON")
    check("insert_rows row count", count("TestRows"), 2)

    with ic.insert_cursor_xy("TestRawXY", [("name", "STRING")], ws="memory",
                             spatial_reference=SR) as cur:
        cur.insertRow((5.0, 6.0, "p"))
    check("insert_cursor_xy row count", count("TestRawXY"), 1)

    ic.insert_rows_xy([(1.0, 2.0, "a"), (3.0, 4.0, "b")], "TestRowsXY",
                      [("name", "STRING")], ws="memory", spatial_reference=SR)
    check("insert_rows_xy row count", count("TestRowsXY"), 2)

    # ---------------------------------------------------------------- insert_df_hex guard
    # gridhex (https://github.com/mraad/grid-hex) is GitHub-only, not on PyPI, so when it is
    # absent all this file can check is that the guard fires with an actionable message. The
    # functional insert path lives in t_insert_df_hex.py, which SKIPs when gridhex is missing
    # so the gap is visible in the summary rather than hidden behind a PASS here.
    if not ic.gridhex_imported:
        hexes = spark.createDataFrame(
            [(0, 5)], schema=StructType([StructField("nume", LongType()),
                                         StructField("pop", LongType())]))
        report("gridhex", "not installed - checking the guard only")
        raised = None
        try:
            ic.insert_df_hex(hexes, "TestHex", 100.0, ws="memory")
        except AssertionError as ex:  # the guard we want
            raised = ex
        # Assert OUTSIDE the try, so a sentinel cannot be caught by our own except clause.
        assert raised is not None, "insert_df_hex must assert when gridhex is missing"
        assert "grid-hex" in str(raised), f"guard message should name the package: {raised}"
        print("  ok  insert_df_hex guards on missing gridhex")

    cleanup("TestBins", "TestWkt", "TestPoints", "TestProgress",
            "TestRaw", "TestRows", "TestRawXY", "TestRowsXY")
finally:
    stop()

print("PASS")
