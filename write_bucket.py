from datetime import datetime
from dv_utils import audit_log, LogLevel
import gcs 
import config
import util
import os
import json
import dv_secret_manager

def write_data(data: dict, location: str = None, secret_manager_key: str = None, file_name: str = "export_data"):
  audit_log("creating tmp file")
  tmp_file = util.create_tmp_json(data)
  audit_log("connecting to gcs")

  loc = location if location and len(location) else config.GCS_DEFAULT_WRITE
  ext = "{model}.json"
  s_key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY

  gcs_conn, duckdb_conn = gcs.connect(f"{loc}/{ext}", s_key)

  audit_log("create duckdb table")
  duckdb_conn.sql(f"CREATE TABLE {file_name} AS SELECT * FROM read_json('{tmp_file}')")
  
  audit_log("export data to gcs", LogLevel.INFO)
  gcs_conn.export_duckdb(file_name)
  audit_log("exported data", LogLevel.INFO)
  os.remove(tmp_file)
  pass

def write_signed_data(data: dict, location: str = None, secret_manager_key: str = None):
  final_data = dict()
  final_data['data'] = data
  signature = __sign_data(data)
  final_data['signature'] = signature

  write_data(final_data, location, secret_manager_key, "export_data_signed")

def __sign_data(data: dict) -> dict:
  data_bytes = json.dumps(data).encode("utf-8")
  configuration = dv_secret_manager.Configuration(host = config.SECRET_MANAGER_URL)

  with dv_secret_manager.ApiClient(configuration) as c:
    inst = dv_secret_manager.DefaultApi(c)
    try:
      resp = inst.sign_post(data_bytes)
      audit_log("signed data at secret manager")
      return resp
    except Exception as e:
      audit_log(f"could not sign data: {e}", LogLevel.ERROR)
      return None