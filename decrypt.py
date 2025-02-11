from dv_utils import audit_log, LogLevel
import json
import util
import gcs
import os
import io
import requests
import config

def decrypt_file(message:dict, location: str = None, secret_manager_key: str = None):
  file_like_obj = io.StringIO(json.dumps(message))
  files = {'message': file_like_obj}

  resp = requests.post(f"{config.SECRET_MANAGER_URL}/decrypt", files=files)
  if not resp.ok:
    audit_log(f"could not decrypt file. Got [{resp.status_code}]: {resp.text}")
    return
  
  resp_text = resp.text
  resp_json = {"content": resp_text}
  tmp_file = util.create_tmp_json(resp_json)

  loc = location if location and len(location) else config.GCS_DEFAULT_WRITE
  ext = "{model}.json"
  s_key = secret_manager_key if secret_manager_key else config.SECRET_MANAGER_KEY

  gcs_conn, duckdb_conn = gcs.connect(f"{loc}/{ext}", s_key)

  duckdb_conn.sql(f"CREATE TABLE decrypted_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_duckdb("decrypted_data")
  audit_log("exported decrypted data to gcs", LogLevel.INFO)
  os.remove(tmp_file)