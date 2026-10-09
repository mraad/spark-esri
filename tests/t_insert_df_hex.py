"""Functional test for insert_cursor.insert_df_hex - the one path that needs gridhex.

Split out of t_insert_cursor.py deliberately. gridhex (https://github.com/mraad/grid-hex)
is GitHub-only and not on PyPI, so on most machines this SKIPs. Keeping it in its own file
means the gap shows up as a SKIP in the run_all.py summary instead of being hidden behind a
PASS on a file whose other assertions all ran.

t_insert_cursor.py still checks that insert_df_hex's missing-gridhex guard fires.
"""
import arcpy

from _harness import check, report, require, start, stop  # first: puts python/ on sys.path

import spark_esri  # noqa: F401 - puts Pro's bundled pyspark on sys.path when it is not pip installed
from pyspark.sql.types import LongType, StructField, StructType

gridhex = require("gridhex", "Install from https://github.com/mraad/grid-hex to enable this test.")

import insert_cursor as ic

SIZE = 100.0

spark, sql = start()
try:
    report("gridhex", getattr(gridhex, "__file__", "?"))

    # nume is the packed hex index gridhex.Hex.from_nume() understands.
    hexes = spark.createDataFrame(
        [(0, 5), (1, 7)],
        schema=StructType([StructField("nume", LongType()), StructField("pop", LongType())]))

    ic.insert_df_hex(hexes, "TestHex", SIZE, ws="memory")

    check("insert_df_hex row count", int(arcpy.management.GetCount("memory/TestHex")[0]), 2)
    check("insert_df_hex shape type", arcpy.Describe("memory/TestHex").shapeType, "Polygon")

    with arcpy.da.SearchCursor("memory/TestHex", ["SHAPE@", "pop"]) as cur:
        rows = [(r[0], r[1]) for r in cur]
    check("insert_df_hex attributes", sorted(p for _, p in rows), [5, 7])

    # Each row must be a real hexagon, not an empty/degenerate shape.
    for shape, pop in rows:
        assert shape is not None, f"null geometry for pop={pop}"
        assert shape.area > 0.0, f"degenerate hex for pop={pop}: area={shape.area}"
        # A hexagon ring is 7 points (first repeated to close it).
        point_count = shape.pointCount
        assert point_count in (6, 7), f"expected a 6-sided hex, got {point_count} points"
    report("hex area", round(rows[0][0].area, 2))

    arcpy.management.Delete("memory/TestHex")
finally:
    stop()

print("PASS")
