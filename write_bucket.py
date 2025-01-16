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

def write_data(data: dict, key_id: str, secret: str):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = __create_tmp_json(data)
  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  duckdb_conn.sql(f"CREATE TABLE export_data AS SELECT * FROM read_json('{tmp_file}')")
  
  gcs_conn.export_duckdb("export_data")
  audit_log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass

def write_signed_data(data: dict, should_download: bool, key_id: str, secret: str):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = __create_tmp_json(data)
  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  duckdb_conn.sql(f"CREATE TABLE signed_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_signed_output_duckdb("signed_data", "michiel")
  audit_log("exported signed data", LogLevel.INFO)

  os.remove(tmp_file)

  if should_download:
    audit_log("downloading signed data", LogLevel.INFO)
    output_path = os.path.join(config.DATA_FOLDER, "signed_data.json")
    query = f"COPY signed_json_signed_data TO '{output_path}'"
    duckdb_conn.sql(query)
    audit_log("downloaded signed data")