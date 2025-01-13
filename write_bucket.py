from datetime import datetime
from dv_utils import audit_log, LogLevel
import gcs 
import config
import json
import os

def write_data(data: dict):
  data['timestamp'] = datetime.now().isoformat()
  gcs_conn, duckdb_conn = gcs.connect_gcs(config.OUTPUT_CONNECTOR)

  tmp_file = os.path.join(config.DATA_FOLDER, "tmp.json")
  with open(tmp_file, 'w') as f:
    f.write(json.dumps(data))

  duckdb_conn.sql(f"CREATE TABLE export_data AS SELECT * FROM read_json('{str(tmp_file)}')")
  
  gcs_conn.export_duckdb("export_data")
  audit_log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass