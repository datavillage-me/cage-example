from dv_utils import SecretManager, log, LogLevel
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
    log(f"could not decrypt file. Got [{resp.status_code}]: {resp.text}")
    return
  
  resp_text = resp.text
  resp_json = {"content": resp_text}
  tmp_file = util.create_tmp_json(resp_json)

  gcs_conn, duckdb_conn = gcs.connect_export(key_id, secret)

  duckdb_conn.sql(f"CREATE TABLE decrypted_data AS SELECT * FROM read_json('{tmp_file}')")

  gcs_conn.export_duckdb("decrypted_data")
  log("exported decrypted data to gcs", LogLevel.INFO)
  os.remove(tmp_file)