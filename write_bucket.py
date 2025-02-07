from datetime import datetime
from dv_utils import log, LogLevel
import gcs 
import config
import util
import os

def write_data(data: dict, key_id: str, secret: str):
  data['timestamp'] = datetime.now().isoformat()
  log("creating tmp file")
  tmp_file = util.create_tmp_json(data)
  log("connecting to gcs")
  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  log("create duckdb table")
  duckdb_conn.sql(f"CREATE TABLE export_data AS SELECT * FROM read_json('{tmp_file}')")
  
  log("export data to gcs")
  gcs_conn.export_duckdb("export_data")
  log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass

def write_signed_data(data: dict, should_download: bool, key_id: str, secret: str):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = util.create_tmp_json(data)
  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  duckdb_conn.sql(f"CREATE TABLE signed_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_signed_output_duckdb("signed_data", "michiel")
  log("exported signed data", LogLevel.INFO)

  os.remove(tmp_file)

  if should_download:
    log("downloading signed data", LogLevel.INFO)
    output_path = os.path.join(config.DATA_FOLDER, "signed_data.json")
    query = f"COPY signed_json_signed_data TO '{output_path}'"
    duckdb_conn.sql(query)
    log("downloaded signed data")