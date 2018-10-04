from iobjectspy import data
from iobjectspy.env import se
from iobjectspy.enums import WorkspaceType


class GeoTiffOp:
    def __init__(self):
        self.work_path = ""
        self.workspace = None


    def open_workspace(self):
        export_to_tif()
        conn = data.WorkspaceConnectionInfo()
        conn.set_server(self.work_path)
        conn.set_type(WorkspaceType.SMWU)
        self.workspace = data.Workspace.open(conn)
