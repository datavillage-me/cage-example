from datetime import datetime
from dv_utils import audit_log, LogLevel
import gcs 
import config
import util
import os

def write_data(data: dict, location: str = None, secret_manager_key: str = None):
  data['timestamp'] = datetime.now().isoformat()
  audit_log("creating tmp file")
  tmp_file = util.create_tmp_json(data)
  audit_log("connecting to gcs")

  loc = location if location and len(location) else config.GCS_DEFAULT_WRITE
  ext = "{model}.json"
  s_key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY

  gcs_conn, duckdb_conn = gcs.connect(f"{loc}/{ext}", s_key)

  audit_log("create duckdb table")
  duckdb_conn.sql(f"CREATE TABLE export_data AS SELECT * FROM read_json('{tmp_file}')")
  
  audit_log("export data to gcs")
  gcs_conn.export_duckdb("export_data")
  audit_log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass

def write_signed_data(data: dict, location: str = None, secret_manager_key: str = None):
  data['timestamp'] = datetime.now().isoformat()
  tmp_file = util.create_tmp_json(data)

  loc = location if location and len(location) else config.GCS_DEFAULT_WRITE
  ext = "{model}.json"
  s_key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY
  gcs_conn, duckdb_conn = gcs.connect(f"{loc}/{ext}", s_key)

  duckdb_conn.sql(f"CREATE TABLE signed_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_signed_output_duckdb("signed_data", "michiel")
  audit_log("exported signed data", LogLevel.INFO)

  os.remove(tmp_file)