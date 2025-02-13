from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
import json
from dv_utils import log, LogLevel
import duckdb
import config
import requests

def __get_conn_secrets(secret_manager_key: str = None) -> dict:
  key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY
  resp = requests.get(f"{config.SECRET_MANAGER_URL}/secrets/{key}?plaintext=true")
  if resp.status_code != 200:
    log(f"unexpected status code returned by secret manager: [{resp.status_code}]: {resp.text}", LogLevel.ERROR)
    return {}
  
  try:
    return json.loads(resp.text)
  except Exception as e:
    log(f"could not parse secret manager response to json: {str(e)}", LogLevel.ERROR)
    return {}
   

def connect(location: str, secret_manager_key: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  creds = __get_conn_secrets(secret_manager_key)

  gcs_config = GCSConfiguration()
  gcs_config.location = location
  gcs_config.file_format = location.split('.')[-1]
  gcs_config.key_id = creds['key_id']
  gcs_config.secret = creds['secret']
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

  return (gcs_conn, duckdb_conn) 
