import codecs

import arcpy
import pyarrow as pa
import pyarrow.parquet as pq

try:
    unicode('')
except NameError:
    unicode = str


class Toolbox(object):
    def __init__(self):
        self.label = "ExportWKTToolbox"
        self.alias = "ExportWKTToolbox"
        self.tools = [TextTool, ParquetTool]


class TextTool(object):
    def __init__(self):
        self.label = "Export To Text File"
        self.description = """
        Export features to a text file with geometry in WKT.
        The WKT is the last field in the output file.
        """
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_inp = arcpy.Parameter(
            name="in_inp",
            displayName="Input Dataset",
            direction="Input",
            datatype="Table View",
            parameterType="Required")

        param_tsv = arcpy.Parameter(
            name="in_tsv",
            displayName="Output TSV File",
            direction="Output",
            datatype="File",
            parameterType="Required")
        # param_tsv.value = "Z:\\Share\\gps-84.wkt"

        param_sep = arcpy.Parameter(
            name="in_sep",
            displayName="Field Separator",
            direction="Input",
            datatype="String",
            parameterType="Required")
        param_sep.value = "tab"

        param_hdr = arcpy.Parameter(
            name="in_hdr",
            displayName="Write Header",
            direction="Input",
            datatype="Boolean",
            parameterType="Required")
        param_hdr.value = True

        param_wkt = arcpy.Parameter(
            name="in_wkt",
            displayName="Write Shape as WKT",
            direction="Input",
            datatype="Boolean",
            parameterType="Required")
        param_wkt.value = True

        param_sp_ref = arcpy.Parameter(
            name="in_sp_ref",
            displayName="Export Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        return [param_inp, param_tsv, param_sep, param_hdr, param_wkt, param_sp_ref]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def safe_unicode(self, obj, *args):
        """ http://code.activestate.com/recipes/466341-guaranteed-conversion-to-unicode-or-byte-string """
        try:
            return unicode(obj, *args)
        except UnicodeDecodeError:
            ascii_text = str(obj).encode("string_escape")
            return unicode(ascii_text)

    def safe_replace(self, obj):
        return self.safe_unicode(obj).replace('\n', ' ').replace('\t', ' ')

    def execute(self, parameters, messages):
        arcpy.env.autoCancelling = False

        fc = parameters[0].valueAsText
        tsv = parameters[1].valueAsText

        description = arcpy.Describe(fc)
        field_names = [field.name for field in description.fields]
        if hasattr(description, "shapeFieldName"):
            shape_name = description.shapeFieldName
            field_names.remove(shape_name)
            if parameters[4].value:
                field_names.append(shape_name + "@WKT")

        sep = parameters[2].valueAsText
        sep = "\t" if sep == "tab" else sep[0]
        with codecs.open(tsv, "wb", "utf-8") as f:
            if parameters[3].value:
                f.write(sep.join(field_names))
                f.write("\n")
            result = arcpy.management.GetCount(fc)
            max_range = int(result.getOutput(0))
            rep_range = max(1, int(max_range / 100))
            arcpy.SetProgressor("step", "Exporting...", 0, max_range, rep_range)
            sp_ref = parameters[5].value
            with arcpy.da.SearchCursor(fc, field_names, spatial_reference=sp_ref) as cursor:
                for pos, row in enumerate(cursor):
                    if pos % rep_range == 0:
                        arcpy.SetProgressorPosition(pos)
                        if arcpy.env.isCancelled:
                            break
                    f.write(sep.join([self.safe_replace(r) for r in row]))
                    f.write("\n")
            arcpy.ResetProgressor()


class ParquetTool(object):
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
