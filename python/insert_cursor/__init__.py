from typing import List, Tuple, Iterable

import os
import arcpy

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import Row, IntegerType, LongType, FloatType, DoubleType, DecimalType
from pyspark.sql.types import DateType, TimestampType, StringType

try:
    # https://github.com/mraad/grid-hex
    from gridhex import Layout, Hex

    gridhex_imported = True
except ImportError:
    gridhex_imported = False


def _df_to_fields(df: DataFrame, index: int) -> List[Tuple[str, str]]:
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
                TimestampType: "LONG"
            }.get(type(field.dataType), "STRING")
            yield field_name, arcpy_type

    return [f for f in yield_field()]


def _insert_cursor(
        cols: List[str],
        name: str,
        fields: List[Tuple[str, str]],
        ws: str,
        spatial_reference: int,
        shape_type: str):
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
        shape_format: str = "WKB"):
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


def insert_rows(rows: Iterable[Row],
                name: str,
                fields: List[Tuple[str, str]],
                ws: str = "memory",
                spatial_reference: int = 3857,
                shape_type: str = "POLYGON",
                shape_format: str = "WKB") -> None:
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
        shape_format: str = "WKB") -> None:
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
    # rows = df.collect()
    rows = df.toLocalIterator()
    insert_rows(rows, name, fields, ws, spatial_reference, shape_type, shape_format)


def insert_cursor_xy(
        name: str,
        fields: List[Tuple[str, str]],
        ws: str = "memory",
        spatial_reference: int = 3857):
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
        spatial_reference: int = 3857) -> None:
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
        spatial_reference: int = 3857) -> None:
    """Create ephemeral point feature class from given dataframe.

    Note - It is assume that the first two data fields are the point x/y values.

    :param df: A dataframe.
    :param name: The name of the feature class.
    :param ws: The feature class workspace. Default="memory".
    :param spatial_reference: The feature class spatial reference. Default=3857.
    """
    fields = _df_to_fields(df, 2)
    # rows = df.collect()
    rows = df.toLocalIterator()
    insert_rows_xy(rows, name, fields, ws, spatial_reference)


def insert_df_hex(
        df: DataFrame,
        name: str,
        size: float,
        ws: str = "memory") -> None:
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
    # rows = df.collect()
    rows = df.toLocalIterator()
    with insert_cursor(name, fields, ws=ws, shape_format="") as cursor:
        for nume, *tail in rows:
            coords = Hex.from_nume(nume).to_coords(layout)
            cursor.insertRow((coords, *tail))
