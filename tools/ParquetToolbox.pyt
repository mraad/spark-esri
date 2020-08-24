import os
from pathlib import Path

import arcpy
import pyarrow as pa
import pyarrow.parquet as pq


class Toolbox(object):
    def __init__(self):
        self.label = "ParquetToolbox"
        self.alias = "ParquetToolbox"
        self.tools = [ExportTool, ImportTool]


class ExportTool(object):
    def __init__(self):
        self.label = "Export To Parquet File"
        self.description = """
        Export a feature class to a parquet file.
        """
        self.canRunInBackground = True

    def getParameterInfo(self):
        tab_view = arcpy.Parameter(
            name="tab_view",
            displayName="Input Dataset",
            direction="Input",
            datatype="Table View",
            parameterType="Required")

        pq_name = arcpy.Parameter(
            name="pq_name",
            displayName="Output Parquet File",
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
        shape_format.value = "WKT"
        shape_format.filter.type = "ValueList"
        shape_format.filter.list = ["WKT", "WKB", "XY"]

        sp_ref = arcpy.Parameter(
            name="sp_ref",
            displayName="Output Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        return [tab_view, pq_name, output_shape, shape_format, sp_ref]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        tab_view = parameters[0].valueAsText
        pq_name = parameters[1].valueAsText

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
        table = pa.Table.from_pydict(py_dict)
        pq.write_table(table, pq_name, version='2.0', flavor='spark')
        arcpy.ResetProgressor()


class ImportTool(object):
    def __init__(self):
        self.label = "Import Parquet Files"
        self.description = "Import Parquet Files"
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
            datatype="GPString",
            parameterType="Required")
        param_path.value = os.path.join("Z:", os.sep)

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

    def execute(self, parameters, messages):
        p_path = parameters[1].value
        p_name = parameters[2].value
        p_x = parameters[3].value
        p_y = parameters[4].value
        p_geom = parameters[5].value
        p_type = parameters[6].value
        p_sp_ref = parameters[7].value

        p = Path(p_path)
        parts = list(p.glob('part-*'))
        if len(parts) == 0:
            arcpy.AddError(f"No part files in {p_path}.")
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

        with open(parts[0], 'rb') as f:
            table = pq.read_table(f)
            schema = table.schema
            for field in schema:
                f_name = field.name
                f_type = field.type
                arcpy.AddMessage(f"field name={f_name} type={f_type}")
                if f_name not in [p_x, p_y, p_geom]:
                    a_type = {
                        'int32': 'INTEGER',
                        'int64': 'LONG',
                        'double': 'DOUBLE'
                    }.get(f_type, 'TEXT')
                    arcpy.management.AddField(fc, f_name, a_type, field_length=256)
                    ap_fields.append(f_name)
                    pq_fields.append(f_name)

        arcpy.env.autoCancelling = False
        with arcpy.da.InsertCursor(fc, ap_fields) as cursor:
            nume = 0
            # warn = 0
            for part in parts:
                if arcpy.env.isCancelled:
                    break
                arcpy.AddMessage(part)
                with open(part, 'rb') as f:
                    table = pq.read_table(f)
                    arcpy.AddMessage(f"Num rows = {table.num_rows}")
                    pydict = table.to_pydict()
                    for i in range(table.num_rows):
                        row = [pydict[c][i] for c in pq_fields]
                        # try:
                        cursor.insertRow(row)
                        nume += 1
                        if nume % 1000 == 0:
                            arcpy.SetProgressorLabel("Imported {} Features...".format(nume))
                            if arcpy.env.isCancelled:
                                break
                        # except Exception as e:
                        #     if arcpy.env.isCancelled:
                        #         break
                        #     if warn < 10:
                        #         warn += 1
                        #         arcpy.AddWarning(str(e))
                        #         if warn == 10:
                        #             arcpy.AddWarning("Too many warnings, will stop reporting them!")
            arcpy.SetProgressorLabel("Imported {} Features.".format(nume))
        symbology = Path(__file__) / f"{p_name}.lyrx"
        if symbology.exists():
            parameters[0].symbology = str(symbology)
        parameters[0].value = fc
        arcpy.ResetProgressor()
