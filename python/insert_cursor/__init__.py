import datetime
import os
from typing import List, Tuple, Iterable, Iterator

import arcpy
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import DateType, TimestampType, TimestampNTZType
from pyspark.sql.types import Row, IntegerType, LongType, FloatType, DoubleType, DecimalType

try:
    # https://github.com/mraad/grid-hex
    from gridhex import Layout, Hex

    gridhex_imported = True
except ImportError:
    gridhex_imported = False


def _df_to_fields(
        df: DataFrame,
        index: int
) -> List[Tuple[str, str]]:
    def yield_field():
        for field in df.schema.fields[index:]:
            field_name = field.name
            arcpy_type = {
                IntegerType: "LONG",
                LongType: "LONG",
                FloatType: "DOUBLE",
                DoubleType: "DOUBLE",
                DecimalType: "DOUBLE",
                DateType: "DATE",
                TimestampType: "DATE",
                TimestampNTZType: "DATE"
            }.get(type(field.dataType), "STRING")
            yield field_name, arcpy_type

    return [f for f in yield_field()]


_EPOCH = datetime.datetime(1970, 1, 1)


def _local_rows(df: DataFrame) -> Iterator[tuple]:
    """df.toLocalIterator(), minus pyspark's crash on pre-1970 timestamps on Windows.

    pyspark turns a timestamp into a datetime with datetime.fromtimestamp(), which on Windows
    raises OSError [Errno 22] for any negative epoch - i.e. any date before 1970, common in
    GIS data. So timestamp columns are shipped as wall-clock microseconds since 1970 instead
    and rebuilt here with timedelta arithmetic, which has no such limit.

    A TimestampType value comes out as its wall clock in spark.sql.session.timeZone, the
    same as toPandas() and as a DataFrame built from pandas expects on the way in. The row
    path used the OS time zone instead; the two only differ if the session zone was changed.
    """
    fields = df.schema.fields
    ts = {i for i, f in enumerate(fields) if isinstance(f.dataType, (TimestampType, TimestampNTZType))}
    if not ts:
        yield from (tuple(row) for row in df.toLocalIterator())
        return
    # Positional names - immune to duplicate or awkward column names.
    names = [f"_c{i}" for i in range(len(fields))]
    cols = [expr(f"timestampdiff(MICROSECOND, timestamp_ntz'1970-01-01 00:00:00', "
                 f"cast({name} as timestamp_ntz))").alias(name) if i in ts else col(name)
            for i, name in enumerate(names)]
    for row in df.toDF(*names).select(*cols).toLocalIterator():
        yield tuple(_EPOCH + datetime.timedelta(microseconds=v) if i in ts and v is not None else v
                    for i, v in enumerate(row))


def _insert_cursor(
        cols: List[str],
        name: str,
        fields: List[Tuple[str, str]],
        ws: str,
        spatial_reference: int,
        shape_type: str
):
    fc = os.path.join(ws, name)
    arcpy.management.Delete(fc)
    sp_ref = arcpy.SpatialReference(spatial_reference)
    arcpy.management.CreateFeatureclass(ws, name, shape_type, spatial_reference=sp_ref)

    for field_name, field_type in fields:
        arcpy.management.AddField(fc, field_name, field_type)
        cols.append(field_name)

    return arcpy.da.InsertCursor(fc, cols)


def insert_cursor(
        name: str,
        fields: List[Tuple[str, str]],
        ws: str = "memory",
        spatial_reference: int = 3857,
        shape_type: str = "POLYGON",
        shape_format: str = "WKB"
):
    """Create and return an ArcPy InsertCursor.

    Note - it is assumed that the first data field is the shape field.

    :param name: The name of the feature class.
    :param fields: List of Tuple[name,type].
    :param ws: The output workspace. Default="memory".
    :param spatial_reference: The spatial reference id. Default=3857.
    :param shape_type: The feature class shape type (POINT,POLYGON,POLYLINE,MULTIPOINT). Default="POLYGON".
    :param shape_format: The shape format (WKB, WKT, ''). Default="WKB".
    :return InsertCursor instance.
    """
    cols = [f"Shape@{shape_format}"]
    return _insert_cursor(cols, name, fields, ws, spatial_reference, shape_type)


def insert_rows(
        rows: Iterable[Row],
        name: str,
        fields: List[Tuple[str, str]],
        ws: str = "memory",
        spatial_reference: int = 3857,
        shape_type: str = "POLYGON",
        shape_format: str = "WKB"
) -> None:
    """Create an ephemeral feature class given collected rows.

    Note - it is assumed that the first data field is the shape field.

    :param rows: The rows to insert.
    :param name: The name of the feature class.
    :param fields: List of Tuple[name,type].
    :param ws: The output workspace. Default="memory".
    :param spatial_reference: The spatial reference id. Default=3857.
    :param shape_type: The feature class shape type (POINT,POLYGON,POLYLINE,MULTIPOINT). Default="POLYGON".
    :param shape_format: The shape format (WKB, WKT, ''). Default="WKB".
    """
    cols = [f"Shape@{shape_format}"]
    with _insert_cursor(cols, name, fields, ws, spatial_reference, shape_type) as cursor:
        for row in rows:
            cursor.insertRow(row)


def insert_df(
        df: DataFrame,
        name: str,
        ws: str = "memory",
        spatial_reference: int = 3857,
        shape_type: str = "POLYGON",
        shape_format: str = "WKB"
) -> None:
    """Create an ephemeral feature class given a dataframe.

    Note - it is assumed that the first data field is the shape field.

    :param df: A dataframe.
    :param name: The name of the feature class.
    :param ws: The output workspace. Default="memory".
    :param spatial_reference: The spatial reference id. Default=3857.
    :param shape_type: The feature class shape type (POINT,POLYGON,POLYLINE,MULTIPOINT). Default="POLYGON".
    :param shape_format: The shape format (WKB, WKT, ''). Default="WKB".
    """
    fields = _df_to_fields(df, 1)
    rows = _local_rows(df)
    insert_rows(rows, name, fields, ws, spatial_reference, shape_type, shape_format)


def insert_cursor_xy(
        name: str,
        fields: List[Tuple[str, str]],
        ws: str = "memory",
        spatial_reference: int = 3857
):
    """Create and return an ArcPy InsertCursor for Point.

    Note - it is assumed than the first two data fields are the x and y values.

    :param name: The name of the feature class.
    :param fields: List of Tuple[name,type].
    :param ws: The output workspace. Default="memory".
    :param spatial_reference: The spatial reference id. Default=3857.
    :return InsertCursor instance.
    """
    cols = ["Shape@X", "SHAPE@Y"]
    return _insert_cursor(cols, name, fields, ws, spatial_reference, "POINT")


def insert_rows_xy(
        rows: Iterable[Row],
        name: str,
        fields: List[Tuple[str, str]],
        ws: str = "memory",
        spatial_reference: int = 3857
) -> None:
    """Create ephemeral point feature class given collected rows.

    :param rows: The rows to insert.
    :param name: The name of the feature class.
    :param fields: List of Tuple[name,type]
    :param ws: The feature class workspace. Default="memory".
    :param spatial_reference: The feature class spatial reference id. Default=3857.
    """
    with insert_cursor_xy(name, fields, ws, spatial_reference) as cursor:
        for row in rows:
            cursor.insertRow(row)


def insert_df_xy(
        df: DataFrame,
        name: str,
        ws: str = "memory",
        spatial_reference: int = 3857
) -> None:
    """Create ephemeral point feature class from given dataframe.

    Note - It is assumed that the first two data fields are the point x/y values.

    :param df: A dataframe.
    :param name: The name of the feature class.
    :param ws: The feature class workspace. Default="memory".
    :param spatial_reference: The feature class spatial reference. Default=3857.
    """
    fields = _df_to_fields(df, 2)
    rows = _local_rows(df)
    insert_rows_xy(rows, name, fields, ws, spatial_reference)


def insert_df_hex(
        df: DataFrame,
        name: str,
        size: float,
        ws: str = "memory"
) -> None:
    """Create ephemeral polygon feature class from given dataframe.

    Note - It is assumed that the first field is the hex nume value.

    :param df: A dataframe.
    :param name: The name of the feature class.
    :param size: The hex size in meters.
    :param ws: The feature class workspace. Default="memory".
    """
    assert gridhex_imported, "Install gridhex module from https://github.com/mraad/grid-hex"
    layout = Layout(size)
    fields = _df_to_fields(df, 1)
    rows = _local_rows(df)
    with insert_cursor(name, fields, ws=ws, shape_format="") as cursor:
        for nume, *tail in rows:
            coords = Hex.from_nume(nume).to_coords(layout)
            cursor.insertRow((coords, *tail))


def insert_df_progress(
        df: DataFrame,
        name: str,
        ws: str = "memory",
        spatial_reference: int = 3857,
        shape_type: str = "POLYGON",
        shape_format: str = "WKB"
) -> str:
    """Create an ephemeral feature class given a dataframe and update pro progress bar.

    Note - it is assumed that the first column is the shape field.

    :param df: A dataframe.
    :param name: The name of the feature class.
    :param ws: The output workspace. Default="memory".
    :param spatial_reference: The spatial reference id. Default=3857.
    :param shape_type: The feature class shape type (POINT,POLYGON,POLYLINE,MULTIPOINT). Default="POLYGON".
    :param shape_format: The shape format (WKB, WKT, ''). Default="WKB".
    :return The name of the feature class. None if the user clicked the cancel button.
    """
    # Take over cancellation handling for the duration of the insert - the loop below polls
    # arcpy.env.isCancelled itself. Restore the caller's setting on the way out, otherwise
    # every later arcpy call in this process silently runs with auto-cancel disabled.
    previous_auto_cancelling = arcpy.env.autoCancelling
    arcpy.env.autoCancelling = False
    try:
        fields = _df_to_fields(df, 1)
        rows = list(_local_rows(df))
        ws_name = os.path.join(ws, name)
        if not arcpy.env.isCancelled:
            max_range = len(rows)
            rep_range = max(1, max_range // 1000)
            arcpy.SetProgressor("step", f"Inserting {max_range} feature(s)...", 0, max_range, rep_range)
            cols = [f"Shape@{shape_format}"]
            try:
                with _insert_cursor(cols, name, fields, ws, spatial_reference, shape_type) as cursor:
                    for pos, row in enumerate(rows):
                        if pos % rep_range == 0:
                            # Update the progress bar.
                            arcpy.SetProgressorPosition(pos)
                            # Check for user cancel.
                            if arcpy.env.isCancelled:
                                break
                        cursor.insertRow(row)
            finally:
                arcpy.ResetProgressor()
        else:
            ws_name = None
        return ws_name
    finally:
        arcpy.env.autoCancelling = previous_auto_cancelling
