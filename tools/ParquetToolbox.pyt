import os
import re
from pathlib import Path
from urllib.parse import urlparse

import arcpy

try:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import fs
    from s3fs import S3FileSystem

    pyarrow_found = True
    pyarrow_error = None
except ModuleNotFoundError:
    pyarrow_found = False
    pyarrow_error = """
    Please execute 'pip install -U boto3==1.16.0 pyarrow==2.0.0 s3fs==0.4.2' in the ArcGIS Python Command Prompt. 
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
        Parquet folder can be on local file system on on S3.
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

        sp_ref = arcpy.Parameter(
            name="sp_ref",
            displayName="Output Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        batch_size = arcpy.Parameter(
            name="batch_size",
            displayName="Batch Size",
            direction="Input",
            datatype="GPLong",
            parameterType="Required")
        batch_size.value = 1_00_000

        return [tab_view, pq_name, output_shape, shape_format, sp_ref, batch_size]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        if not pyarrow_found:
            arcpy.AddError(pyarrow_error)
            return

        tab_view = parameters[0].valueAsText
        pq_path = parameters[1].value
        py_size = parameters[5].value

        description = arcpy.Describe(tab_view)
        field_names = [field.name for field in description.fields]
        if hasattr(description, "shapeFieldName"):
            shape_name = description.shapeFieldName
            field_names.remove(shape_name)
            if parameters[2].value:
                shape_format = parameters[3].value
                if shape_format == "XY":
                    field_names.append(shape_name + "@X")
                    field_names.append(shape_name + "@Y")
                else:
                    field_names.append(shape_name + "@" + shape_format)

        sections = urlparse(pq_path)
        is_s3 = sections.scheme == "s3"
        if is_s3:
            if "AWS_ACCESS_KEY_ID" not in os.environ or "AWS_SECRET_ACCESS_KEY" not in os.environ:
                arcpy.AddError("Missing environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
                return
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
                client_kwargs['endpoint_url'] = os.getenv("AWS_ENDPOINT_URL")
                put_object = False
            else:
                put_object = True
            filesystem = S3FileSystem(anon=False,
                                      key=os.getenv("AWS_ACCESS_KEY_ID"),
                                      secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                      client_kwargs=client_kwargs
                                      )
            if filesystem.exists(f"{bucket}/{base_path}"):
                arcpy.AddError(f"Object {bucket}/{base_path} already exists !")
                return
            if put_object:
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
        result = arcpy.management.GetCount(tab_view)
        max_range = int(result.getOutput(0))
        rep_range = max(1, int(max_range / 100))
        arcpy.SetProgressor("step", "Exporting...", 0, max_range, rep_range)
        sp_ref = parameters[4].value
        arcpy.env.autoCancelling = False
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
                    table = pa.Table.from_pydict(py_dict)
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
        if py_nume > 0:
            table = pa.Table.from_pydict(py_dict)
            part_name = f"part-{pq_part:05d}.parquet"
            where = f"{pq_path}{part_name}" if is_s3 else os.path.join(pq_path, part_name)
            pq.write_table(table,
                           where,
                           filesystem=filesystem,
                           version="2.0",
                           flavor="spark")

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

        param_sp_ref = arcpy.Parameter(
            name="in_sp_ref",
            displayName="Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        field_x = arcpy.Parameter(
            name="field_x",
            displayName="X Field",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        field_y = arcpy.Parameter(
            name="field_y",
            displayName="Y Field",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        field_wkt = arcpy.Parameter(
            name="field_wkt",
            displayName="WKB Field",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")

        p_type = arcpy.Parameter(
            name="geom_type",
            displayName="Geometry Type",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        p_type.filter.type = "ValueList"
        p_type.filter.list = ["POINT", "POLYLINE", "POLYGON", "MULTIPOINT"]
        p_type.value = "POINT"

        param_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        param_memory.value = False

        return [out_fc,
                param_path,
                param_name,
                field_x,
                field_y,
                field_wkt,
                p_type,
                param_sp_ref,
                param_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        if not pyarrow_found:
            arcpy.AddError(pyarrow_error)
            return

        p_path = parameters[1].valueAsText
        p_name = parameters[2].value
        p_x = parameters[3].value
        p_y = parameters[4].value
        p_geom = parameters[5].value
        p_type = parameters[6].value
        p_sp_ref = parameters[7].value

        sections = urlparse(p_path)
        base_path = sections.path[1:]
        if sections.scheme == "s3":
            if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
                bucket = sections.hostname
                client_kwargs = {}
                if "AWS_ENDPOINT_URL" in os.environ:
                    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
                    client_kwargs['endpoint_url'] = endpoint_url
                    arcpy.AddWarning(f"Using endpoint_url:{endpoint_url}")
                filesystem = S3FileSystem(anon=False,
                                          key=os.getenv("AWS_ACCESS_KEY_ID"),
                                          secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                          client_kwargs=client_kwargs
                                          )
                parts = filesystem.glob(f"{bucket}/{base_path}/part-*")
            else:
                arcpy.AddError("Missing environment variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'.")
                return
        else:
            filesystem = fs.LocalFileSystem()
            p = Path(p_path)
            if p.is_file():
                arcpy.AddError(f"{p_path} is not a folder. Make sure all the files in {p_path} start with 'part-'.")
                return
            parts = list(p.glob('part-*'))

        if len(parts) == 0:
            arcpy.AddError(f"Cannot find files in {p_path} that start with 'part-'.")
            return

        ws = "memory" if parameters[8].value else arcpy.env.scratchGDB
        fc = os.path.join(ws, p_name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        is_fc = True
        if p_geom is not None:
            ap_fields = ['SHAPE@WKB']
            pq_fields = [p_geom]
        elif p_x is not None and p_y is not None:
            ap_fields = ['SHAPE@X', 'SHAPE@Y']
            pq_fields = [p_x, p_y]
        else:
            ap_fields = []
            pq_fields = []
            is_fc = False

        if is_fc:
            arcpy.management.CreateFeatureclass(
                ws,
                p_name,
                p_type,
                spatial_reference=p_sp_ref,
                has_m="DISABLED",
                has_z="DISABLED")
        else:
            arcpy.management.CreateTable(ws, p_name)

        prog = re.compile(r"""^\d""")
        object_id = 1
        table = pq.read_table(parts[0], filesystem=filesystem)
        schema = table.schema
        for field in schema:
            p_name = field.name
            if p_name == "OBJECTID":
                a_name = f"OBJECTID_{object_id}"
                object_id += 1
            elif prog.match(p_name):
                a_name = "F" + p_name
            else:
                a_name = p_name
            f_type = str(field.type)
            arcpy.AddMessage(f"field name={p_name} type={f_type}")
            if p_name not in [p_x, p_y, p_geom]:
                a_type = {
                    'int32': 'INTEGER',
                    'int64': 'LONG',
                    'float': 'DOUBLE',
                    'double': 'DOUBLE'
                }.get(f_type, 'TEXT')
                arcpy.management.AddField(fc, a_name, a_type, field_alias=p_name, field_length=1024)
                ap_fields.append(a_name)
                pq_fields.append(p_name)

        arcpy.env.autoCancelling = False
        with arcpy.da.InsertCursor(fc, ap_fields) as cursor:
            nume = 0
            for part in parts:
                if arcpy.env.isCancelled:
                    break
                table = pq.read_table(part, filesystem=filesystem)
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
            arcpy.SetProgressorLabel(f"Imported {nume} Features.")
        symbology = Path(__file__) / f"{p_name}.lyrx"
        if symbology.exists():
            parameters[0].symbology = str(symbology)
        parameters[0].value = fc
        arcpy.ResetProgressor()
