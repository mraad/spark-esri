#
# proswap arcgispro-py3
# conda remove --yes --all --name sparkgeo
# conda create --yes --name sparkgeo --clone arcgispro-py3
# proswap sparkgeo
# pip install boto3 s3fs gcsfs
#
# 2022-09-06: set env var SETUPTOOLS_USE_DISTUTILS=stdlib
#
# https://arrow.apache.org/docs/python/filesystems.html
# https://cloud.google.com/docs/authentication/application-default-credentials
#
import arcpy
import io
import os
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import re
import tempfile
from pathlib import Path
from pyarrow import fs
from typing import List
from urllib.parse import urlparse

try:
    import boto3

    boto3_found = True
    boto3_error = None
except ModuleNotFoundError:
    boto3_found = False
    boto3_error = """
    Please execute 'conda install -c esri -c conda-forge "boto3>=1.17.99"' in an ArcGIS Python Command Prompt. 
    """

try:
    from s3fs import S3FileSystem  # conda install -c esri -c conda-forge "s3fs>=0.5.1"

    s3fs_found = True
except ModuleNotFoundError:
    s3fs_found = False

try:
    import gcsfs

    gcsfs_found = True
    gcsfs_error = None
except ModuleNotFoundError:
    gcsfs_found = False
    gcsfs_error = """
    Please execute 'conda install -c esri -c conda-forge "gcsfs>=0.7.1"' in an ArcGIS Python Command Prompt.
    """


class Toolbox(object):
    def __init__(self):
        self.label = "ParquetToolbox"
        self.alias = "ParquetToolbox"
        self.tools = [ExportTool, ImportTool]


class ExportTool(object):
    def __init__(self):
        self.label = "Export To Parquet"
        self.description = """
        Export a feature class to a parquet folder.
        Parquet folder can be on local file system on an S3 bucket.
        The files in the folder will be named 'part-00001','part-00002',etc...
        """
        self.canRunInBackground = True
        self.tab_view = ""

    def getParameterInfo(self):
        tab_view = arcpy.Parameter(
            name="tab_view",
            displayName="Input Dataset",
            direction="Input",
            datatype="Table View",
            parameterType="Required")

        pq_name = arcpy.Parameter(
            name="pq_name",
            displayName="Output Parquet Folder",
            direction="Output",
            datatype="String",
            parameterType="Required")
        pq_name.value = os.path.join("Z:", os.sep, "delete_me.prq")

        output_shape = arcpy.Parameter(
            name="output_shape",
            displayName="Output Shape",
            direction="Input",
            datatype="Boolean",
            parameterType="Required")
        output_shape.value = True

        shape_format = arcpy.Parameter(
            name="shape_format",
            displayName="Shape Format",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        shape_format.value = "WKB"
        shape_format.filter.type = "ValueList"
        shape_format.filter.list = ["WKT", "WKB", "XY"]

        # sp_ref = arcpy.Parameter(
        #     name="sp_ref",
        #     displayName="Output Spatial Reference",
        #     direction="Input",
        #     datatype="GPSpatialReference",
        #     parameterType="Required")
        # sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        batch_size = arcpy.Parameter(
            name="batch_size",
            displayName="Batch Size",
            direction="Input",
            datatype="GPLong",
            parameterType="Required")
        batch_size.value = 100_000

        return [tab_view, pq_name, output_shape, shape_format, batch_size]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def to_pa_field(self, field):
        field_type = {
            'OID': pa.int64(),
            'Date': pa.timestamp('s'),
            'Double': pa.float64(),
            'Integer': pa.int64(),
            'Single': pa.float32(),
            'SmallInteger': pa.int16(),
            'Blob': pa.large_binary(),
        }.get(field.type, pa.string())
        return pa.field(field.name, field_type)

    def execute(self, parameters, _):
        tab_view = parameters[0].valueAsText
        pq_path = parameters[1].value
        shape_format = parameters[3].value
        py_size = parameters[4].value

        description = arcpy.Describe(tab_view)
        not_type = ('Raster',)
        field_names = [f.name for f in description.fields if f.type not in not_type]
        not_type = ('Raster', 'Geometry')
        schema = pa.schema([self.to_pa_field(f) for f in description.fields if f.type not in not_type])

        if hasattr(description, "shapeFieldName"):
            shape_name = description.shapeFieldName
            field_names.remove(shape_name)
            if parameters[2].value:
                if shape_format == "XY":
                    shape_x = shape_name + "@X"
                    shape_y = shape_name + "@Y"
                    field_names.append(shape_x)
                    field_names.append(shape_y)
                    schema = schema \
                        .append(pa.field(shape_x, pa.float64())) \
                        .append(pa.field(shape_y, pa.float64()))
                else:
                    shape_name = shape_name + "@" + shape_format
                    field_names.append(shape_name)
                    schema = schema.append(pa.field(shape_name, pa.binary()))

        sections = urlparse(pq_path)
        is_s3 = sections.scheme == "s3"
        if is_s3:
            kwargs = {}
            if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
                kwargs["anon"] = False
                kwargs["key"] = os.getenv("AWS_ACCESS_KEY_ID")
                kwargs["secret"] = os.getenv("AWS_SECRET_ACCESS_KEY")
            else:
                arcpy.AddWarning("Missing environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
            bucket = sections.hostname
            if bucket is None:
                arcpy.AddError("Make sure the S3 url is in the format s3://bucket_name/....")
                return
            base_path = sections.path[1:]
            if not base_path.endswith("/"):
                base_path += "/"
                pq_path += "/"
            client_kwargs = {}
            if "AWS_ENDPOINT_URL" in os.environ:
                client_kwargs["endpoint_url"] = os.getenv("AWS_ENDPOINT_URL")
                put_object = False
            else:
                put_object = True
            kwargs["client_kwargs"] = client_kwargs
            filesystem = S3FileSystem(**kwargs)
            if filesystem.exists(f"{bucket}/{base_path}"):
                arcpy.AddError(f"Object {bucket}/{base_path} already exists !")
                return
            if put_object:
                if not boto3_found:
                    arcpy.AddError(boto3_error)
                    return
                try:
                    s3 = boto3.client("s3")
                    s3.put_object(Bucket=bucket, Key=base_path)
                except Exception as e:
                    arcpy.AddError(str(e))
                    return
        else:
            os.makedirs(pq_path, exist_ok=True)
            filesystem = fs.LocalFileSystem()

        pq_part = 0
        py_nume = 0
        py_dict = {k: [] for k in field_names}
        sp_ref = arcpy.env.outputCoordinateSystem
        if sp_ref is None:
            sp_ref = description.spatialReference
        extent = arcpy.env.extent
        clear_selection = False
        if extent is not None:
            clear_selection = True
            extent = extent.projectAs(description.spatialReference)
            polygon = arcpy.Polygon(arcpy.Array(
                [getattr(extent, _) for _ in ('lowerLeft', 'upperLeft', 'upperRight', 'lowerRight', 'lowerLeft')]),
                description.spatialReference
            )
            arcpy.management.SelectLayerByLocation(tab_view, 'INTERSECT', polygon)
        result = arcpy.management.GetCount(tab_view)
        max_range = int(result.getOutput(0))
        rep_range = max(1, max_range // 100)
        arcpy.AddMessage(f"Exporting {max_range} feature(s).")
        arcpy.SetProgressor("step", "Exporting...", 0, max_range, rep_range)
        arcpy.env.autoCancelling = False
        # TODO - Check M and Z output options in env tab.
        with arcpy.da.SearchCursor(tab_view, field_names, spatial_reference=sp_ref) as cursor:
            for pos, row in enumerate(cursor):
                if pos % rep_range == 0:
                    arcpy.SetProgressorPosition(pos)
                    if arcpy.env.isCancelled:
                        break
                for k, v in zip(field_names, row):
                    py_dict[k].append(v)
                py_nume += 1
                if py_nume == py_size:
                    table = pa.Table.from_pydict(py_dict, schema)
                    part_name = f"part-{pq_part:05d}.parquet"
                    where = f"{pq_path}{part_name}" if is_s3 else os.path.join(pq_path, part_name)
                    pq.write_table(table,
                                   where,
                                   filesystem=filesystem,
                                   version="2.0",
                                   flavor="spark")
                    pq_part += 1
                    py_nume = 0
                    py_dict = {k: [] for k in field_names}
        if py_nume > 0 and not arcpy.env.isCancelled:
            table = pa.Table.from_pydict(py_dict, schema)
            part_name = f"part-{pq_part:05d}.parquet"
            where = f"{pq_path}{part_name}" if is_s3 else os.path.join(pq_path, part_name)
            pq.write_table(table,
                           where,
                           filesystem=filesystem,
                           version="2.0",
                           flavor="spark")
        if clear_selection:
            arcpy.management.SelectLayerByAttribute(tab_view, 'CLEAR_SELECTION')
        arcpy.ResetProgressor()


class ImportTool(object):
    def __init__(self):
        self.label = "Import From Parquet"
        self.description = """
        Import parquet files from a folder typically generated from a Spark job.
        The folder can be in a local file system or in an S3 file system.
        The name of the files in the folder HAVE to start with 'part-'
        """
        self.canRunInBackground = True
        self.filesystem = None
        self.bucket = None
        self.s3 = None

    def getParameterInfo(self):
        out_fc = arcpy.Parameter(
            name="out_fc",
            displayName="out_fc",
            direction="Output",
            datatype=["Feature Layer", "Table"],
            parameterType="Derived")

        param_path = arcpy.Parameter(
            name="in_file",
            displayName="Parquet Folder",
            direction="Input",
            datatype=["DEFolder", "String"],
            parameterType="Required")
        # param_path.value = os.path.join("Z:", os.sep)

        param_name = arcpy.Parameter(
            name="in_name",
            displayName="Output Layer Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")

        sp_ref = arcpy.Parameter(
            name="in_sp_ref",
            displayName="Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        field_x = arcpy.Parameter(
            name="field_x",
            displayName="X Column",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        field_y = arcpy.Parameter(
            name="field_y",
            displayName="Y Column",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        field_wkb = arcpy.Parameter(
            name="field_wkb",
            displayName="WKB Column",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        field_col = arcpy.Parameter(
            name="field_col",
            displayName="RegExp of columns to import",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        field_col.value = ".+"

        geom_type = arcpy.Parameter(
            name="geom_type",
            displayName="Geometry Type",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        geom_type.filter.type = "ValueList"
        geom_type.filter.list = ["POINT", "POINT Z", "POINT M",
                                 "POLYLINE", "POLYLINE Z", "POLYLINE M",
                                 "POLYGON", "POLYGON Z", "POLYGON M",
                                 "MULTIPOINT", "MULTIPOINT Z", "MULTIPOINT M"]
        geom_type.value = "POINT"

        in_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        in_memory.value = False

        return [out_fc,
                param_path,
                param_name,
                field_x,
                field_y,
                field_wkb,
                field_col,
                geom_type,
                sp_ref,
                in_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def read_table(self, pq_path: str) -> pa.lib.Table:
        if self.filesystem:
            arcpy.AddMessage(f"Reading {pq_path} using FS...")
            dataset = ds.dataset(pq_path, filesystem=self.filesystem)
            table = dataset.to_table()
        else:
            # arcpy.AddMessage(f"Reading {pq_path} using Boto3")
            buffer = io.BytesIO()
            s3_object = self.s3.Object(self.bucket, pq_path)
            s3_object.download_fileobj(buffer)
            table = pq.read_table(buffer)
        return table

    def glob(self, base_path: str) -> List[str]:
        if self.filesystem:
            arr = self.filesystem.glob(f"{self.bucket}/{base_path}/part-*")
        else:
            prefix = f"{base_path}/part-"
            arr = [item.key for item in self.s3.Bucket(self.bucket).objects.filter(Prefix=prefix)]
        return arr

    def execute(self, parameters, _):
        p_path = parameters[1].valueAsText
        p_name = parameters[2].value
        p_x = parameters[3].value
        p_y = parameters[4].value
        p_geom = parameters[5].value
        p_expr = parameters[6].value
        p_type = parameters[7].value
        p_sp_ref = parameters[8].value
        p_memory = parameters[9].value

        last_symbology = None
        project = arcpy.mp.ArcGISProject("current")
        for l in project.activeMap.listLayers():
            if l.name == p_name:
                last_symbology = os.path.join(tempfile.mkdtemp(), p_name)
                l.saveACopy(last_symbology)
                arcpy.AddMessage(last_symbology)

        sections = urlparse(p_path)
        base_path = sections.path[1:]
        if sections.scheme == "gs":
            if not gcsfs_found:
                arcpy.AddError(gcsfs_error)
                return
            if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
                arcpy.AddError("Environment variable GOOGLE_APPLICATION_CREDENTIALS is missing.")
                return
            self.filesystem = gcsfs.GCSFileSystem(token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            self.bucket = sections.hostname
            parts = self.filesystem.glob(f"{self.bucket}/{base_path}/part-*")
        elif sections.scheme == "s3":
            self.bucket = sections.hostname
            client_kwargs = {}
            if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
                is_minio = False
                if "AWS_ENDPOINT_URL" in os.environ:  # Using MinIO
                    is_minio = True
                    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
                    client_kwargs["endpoint_url"] = endpoint_url
                    arcpy.AddWarning(f"Using environment variable AWS_ENDPOINT_URL ({endpoint_url})")
                if s3fs_found:
                    self.filesystem = S3FileSystem(anon=False,
                                                   key=os.getenv("AWS_ACCESS_KEY_ID"),
                                                   secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                                   client_kwargs=client_kwargs
                                                   )
                else:
                    if not boto3_found:
                        arcpy.AddError(boto3_error)
                        return
                    if is_minio:
                        client_kwargs["config"] = boto3.session.Config(signature_version="s3v4")
                    self.s3 = boto3.resource("s3", **client_kwargs)
            else:
                # Case when Pro is running in AWS
                self.filesystem = S3FileSystem()
            parts = self.glob(base_path)
        else:
            self.filesystem = fs.LocalFileSystem()
            p = Path(p_path)
            if p.is_file():
                arcpy.AddError(f"{p_path} is not a folder. Make sure all the files in {p_path} start with 'part-'.")
                return
            parts = list(p.glob("part-*"))

        if len(parts) == 0:
            arcpy.AddError(f"Cannot find files in '{p_path}' that start with 'part-'.")
            return

        ws = "memory" if p_memory else arcpy.env.scratchGDB
        fc = os.path.join(ws, p_name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        is_feature_class = True
        if p_geom is not None:
            ap_fields = ["SHAPE@WKB"]
            pq_fields = [p_geom]
            p_expr = f"{p_expr}|{p_geom}"
        elif p_x is not None and p_y is not None:
            ap_fields = ["SHAPE@X", "SHAPE@Y"]
            pq_fields = [p_x, p_y]
            p_expr = f"{p_expr}|{p_x}|{p_y}"
        else:
            ap_fields = []
            pq_fields = []
            is_feature_class = False

        if is_feature_class:
            geom_type, has_m, has_z = {
                "POINT": ("POINT", "DISABLED", "DISABLED"),
                "POINT M": ("POINT", "ENABLED", "DISABLED"),
                "POINT Z": ("POINT", "DISABLED", "ENABLED"),
                "POLYLINE": ("POLYLINE", "DISABLED", "DISABLED"),
                "POLYLINE M": ("POLYLINE", "ENABLED", "DISABLED"),
                "POLYLINE Z": ("POLYLINE", "DISABLED", "ENABLED"),
                "POLYGON": ("POLYGON", "DISABLED", "DISABLED"),
                "POLYGON M": ("POLYGON", "ENABLED", "DISABLED"),
                "POLYGON Z": ("POLYGON", "DISABLED", "ENABLED"),
                "MULTIPOINT": ("MULTIPOINT", "DISABLED", "DISABLED"),
                "MULTIPOINT M": ("MULTIPOINT", "ENABLED", "DISABLED"),
                "MULTIPOINT Z": ("MULTIPOINT", "DISABLED", "ENABLED")
            }[p_type]
            arcpy.management.CreateFeatureclass(
                ws,
                p_name,
                geom_type,
                spatial_reference=p_sp_ref,
                has_m=has_m,
                has_z=has_z)
        else:
            arcpy.management.CreateTable(ws, p_name)

        # arcpy.AddMessage(f"Cols regexp {p_expr}")
        prog = re.compile(r"""^\d""")
        expr = re.compile(p_expr)
        object_id = 1
        table = self.read_table(parts[0])
        schema = table.schema
        for field in schema:
            f_name = field.name
            if expr.match(f_name):
                if f_name.upper() == "OBJECTID":
                    a_name = f"OBJECTID_{object_id}"
                    object_id += 1
                elif prog.match(f_name):  # Check for field names that start with a digit
                    a_name = "F" + f_name
                else:
                    a_name = f_name
                f_type = str(field.type)[:5]
                arcpy.AddMessage(f"col name={f_name} type={f_type}")
                if f_name not in [p_x, p_y, p_geom]:
                    a_type = {
                        "int32": "INTEGER",
                        "int64": "LONG",
                        "float": "DOUBLE",
                        "doubl": "DOUBLE",
                        "times": "DATE",
                        "decim": "DOUBLE"
                    }.get(f_type, "TEXT")
                    arcpy.management.AddField(fc, a_name, a_type,
                                              field_alias=f_name,
                                              field_is_nullable="NULLABLE",
                                              field_length=512)
                    ap_fields.append(a_name)
                    pq_fields.append(f_name)

        arcpy.env.autoCancelling = False
        arcpy.SetProgressor("step", "Importing...", 0, len(parts), 1)
        nume = 0
        with arcpy.da.InsertCursor(fc, ap_fields) as cursor:
            for pos, part in enumerate(parts):
                arcpy.SetProgressorPosition(pos)
                if arcpy.env.isCancelled:
                    break
                table = self.read_table(part)
                arcpy.AddMessage(f"{part} rows={table.num_rows}")
                pydict = table.to_pydict()
                for i in range(table.num_rows):
                    row = [pydict[c][i] for c in pq_fields]
                    cursor.insertRow(row)
                    nume += 1
                    if nume % 1000 == 0:
                        arcpy.SetProgressorLabel(f"Imported {nume} Features...")
                        if arcpy.env.isCancelled:
                            break
        if not arcpy.env.isCancelled:
            arcpy.SetProgressorLabel(f"Imported {nume} Features.")
            parameters[0].value = fc
            if last_symbology:
                parameters[0].symbology = f"{last_symbology}.lyrx"
            else:
                symbology = Path(__file__).parent / f"{p_name}.lyrx"
                if symbology.exists():
                    parameters[0].symbology = str(symbology)
        arcpy.ResetProgressor()
