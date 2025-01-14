from datetime import datetime
from dv_utils import audit_log, LogLevel
import gcs 
import config
import json
import os

def __create_tmp_json(data: dict) -> str:
  tmp_file = os.path.join(config.DATA_FOLDER, 'tmp.json')
  with open(tmp_file, 'w') as f:
    f.write(json.dumps(data))
  
  return tmp_file

def write_data(data: dict):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = __create_tmp_json(data)
  gcs_conn, duckdb_conn = gcs.connect_gcs(config.OUTPUT_CONNECTOR)

  duckdb_conn.sql(f"CREATE TABLE export_data AS SELECT * FROM read_json('{tmp_file}')")
  
  gcs_conn.export_duckdb("export_data")
  audit_log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass

def write_signed_data(data: dict):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = __create_tmp_json(data)
  gcs_conn, duckdb_conn = gcs.connect_gcs(config.OUTPUT_CONNECTOR)

  duckdb_conn.sql(f"CREATE TABLE signed_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_signed_output_duckdb("signed_data", "michiel")
  audit_log("exported signed data", LogLevel.INFO)