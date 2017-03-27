import arcpy
import io
import os
import re
import requests
import sys


class Toolbox(object):
    def __init__(self):
        self.label = "AlluxioToolbox"
        self.alias = "AlluxioToolbox"
        self.tools = [ImportTrackTool, ExportTargetsTool]


class ImportTrackTool(object):
    def __init__(self):
        self.label = "Import Tracks"
        self.description = """
        Import Tracks in WKT format from an Alluxio based folder
        """
        self.canRunInBackground = True
        self.insertCount = 0
        self.exceptCount = 0

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="out_fc",
            displayName="Tracks",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_name = arcpy.Parameter(
            name="in_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = "Tracks"

        param_host = arcpy.Parameter(
            name="in_host",
            displayName="Alluxio Host",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_host.value = "quickstart"

        param_path = arcpy.Parameter(
            name="in_path",
            displayName="Path",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_path.value = "/tracks"

        return [param_fc, param_name, param_host, param_path]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def insert_row(self, cursor, line):
        tokens = line.split("\t")
        cursor.insertRow(tokens)
        self.insertCount += 1

    def execute(self, parameters, messages):
        name = parameters[1].value
        host = parameters[2].value
        path = parameters[3].value

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + name
        else:
            fc = os.path.join(arcpy.env.scratchGDB, name)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        sp_ref = arcpy.SpatialReference(4326)
        arcpy.management.CreateFeatureclass(ws, name, "POLYLINE", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "TRACK_NUM", "STRING")
        arcpy.management.AddField(fc, "HEAD_DATE", "STRING")
        arcpy.management.AddField(fc, "LAST_DATE", "STRING")
        arcpy.management.AddField(fc, "TRACK_SEC", "LONG")

        with arcpy.da.InsertCursor(fc, ["SHAPE@WKT", "TRACK_NUM", "HEAD_DATE", "LAST_DATE", "TRACK_SEC"]) as cursor:
            try:
                url = "http://{}:19999/api/v1/master/file/list_status".format(host)
                res = requests.get(url, params={"path": path})
                docs = res.json()
                arcpy.SetProgressor("step", "Importing...", 0, len(docs), 1)
                for pos, doc in enumerate(docs):
                    arcpy.SetProgressorLabel(doc["path"])
                    arcpy.SetProgressorPosition(pos)

                    url = "http://{}:39999/api/v1/paths/{}/open-file".format(host, doc["path"])
                    res = requests.post(url)
                    sid = res.text

                    url = "http://{}:39999/api/v1/streams/{}/read".format(host, sid)
                    res = requests.post(url)
                    for line in io.StringIO(res.text):
                        self.insert_row(cursor, line)

                    url = "http://{}:39999/api/v1/streams/{}/close".format(host, sid)
                    requests.post(url)
                arcpy.ResetProgressor()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                arcpy.AddError("{} {}".format(e, exc_tb.tb_lineno))
                del (exc_type, exc_obj, exc_tb)

        parameters[0].value = fc

        if in_memory:
            arcpy.AddMessage("Added {} features".format(self.insertCount))
            if self.exceptCount > 0:
                arcpy.AddError("Found {} parsing errors".format(self.exceptCount))


class ExportTargetsTool(object):
    def __init__(self):
        self.label = "Export Targets"
        self.description = """
        Export targets to an Alluxio based file in WKT format
        """
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_inp = arcpy.Parameter(name="in_inp",
                                    displayName="Input Dataset",
                                    direction="Input",
                                    datatype="Table View",
                                    parameterType="Required")

        param_host = arcpy.Parameter(name="in_host",
                                     displayName="Alluxio Host",
                                     direction="Output",
                                     datatype="String",
                                     parameterType="Required")
        param_host.value = "quickstart"

        param_path = arcpy.Parameter(name="in_path",
                                     displayName="Alluxio Path",
                                     direction="Input",
                                     datatype="String",
                                     parameterType="Required")
        param_path.value = "/Broadcast.csv"

        param_sr = arcpy.Parameter(name="in_sr",
                                   displayName="Export Spatial Reference",
                                   direction="Input",
                                   datatype="GPSpatialReference",
                                   parameterType="Required")

        return [param_inp, param_host, param_path, param_sr]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def data(self, inp, sp_ref):
        result = arcpy.management.GetCount(inp)
        max_range = int(result.getOutput(0))
        rep_range = max(1, int(max_range / 30))
        arcpy.SetProgressor("step", "Exporting...", 0, max_range, 1)

        description = arcpy.Describe(inp)
        field_names = [field.name for field in description.fields]
        if hasattr(description, 'shapeFieldName'):
            shape_name = description.shapeFieldName
            field_names.remove(shape_name)
            field_names.append(shape_name + "@WKT")

        with arcpy.da.SearchCursor(inp, field_names, spatial_reference=sp_ref) as cursor:
            str_io = io.StringIO()
            inc_range = 0
            rec_range = 0
            for row in cursor:
                line = "\t".join([str(r) for r in row])
                str_io.write(line)
                str_io.write("\n")
                rec_range += 1
                inc_range += 1
                if inc_range == rep_range:
                    inc_range = 0
                    arcpy.SetProgressorPosition(rec_range)
                    text = str_io.getvalue()
                    str_io.close()
                    str_io = io.StringIO()
                    yield text.encode()
            if inc_range > 0:
                text = str_io.getvalue()
                yield text.encode()
            str_io.close()
            arcpy.AddMessage("Wrote {} records".format(rec_range))
        arcpy.ResetProgressor()

    def execute(self, parameters, messages):
        inp = parameters[0].valueAsText
        host = parameters[1].valueAsText
        path = parameters[2].valueAsText
        sp_ref = parameters[3].value

        url = "http://{}:39999/api/v1/paths/{}/create-file".format(host, path)
        res = requests.post(url)
        sid = res.text
        if re.search("(\d+)", sid):
            hdr = {
                "Content-Type": "application/octet-stream"
            }
            url = "http://{}:39999/api/v1/streams/{}/write".format(host, sid)
            res = requests.post(url, headers=hdr, data=self.data(inp, sp_ref))
            arcpy.AddMessage(res.text)

            url = "http://{}:39999/api/v1/streams/{}/close".format(host, sid)
            res = requests.post(url)
            arcpy.AddMessage(res.text)
        else:
            arcpy.AddError(sid)
