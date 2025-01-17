from dv_utils import SecretManager, audit_log, LogLevel
import json
import util
import gcs
import os
import io
import requests
import config

def decrypt_file(message:dict, key_id: str, secret: str):
  file_like_obj = io.StringIO(json.dumps(message))
  files = {'message': file_like_obj}

  resp = requests.post(f"{config.SECRET_MANAGER_URL}/decrypt", files=files)
  if not resp.ok:
    audit_log(f"could not decrypt file. Got [{resp.status_code}]: {resp.text}")
    return
  
  resp_obj = resp.json()
  tmp_file = util.create_tmp_json(resp_obj)

  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  duckdb_conn.sql(f"CREATE TABLE decrypted_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_duckdb("decrypted_data")
  audit_log("exported decrypted data to gcs", LogLevel.INFO)
  os.remove(tmp_file)