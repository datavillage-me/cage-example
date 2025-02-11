from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
import dv_secret_manager
import json
from dv_utils import audit_log, LogLevel
import duckdb
import config

def __get_conn_secrets(secret_manager_key: str = None) -> dict:
  conf = dv_secret_manager.Configuration(host = "http://localhost:8081")
  key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY

  with dv_secret_manager.ApiClient(conf) as c:
    instance = dv_secret_manager.DefaultApi(c)
    try:
      resp = instance.secrets_secret_get(key, plaintext=True)
      return json.loads(resp)
    except Exception as e:
      audit_log(f"could not get secrets: {e}", LogLevel.ERROR)
      return dict()
   

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

# def connect_import(location: str = None, secret_manager_key: str = None) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
#   secrets = __get_conn_secrets(secret_manager_key)

#   gcs_config = GCSConfiguration()

#   loc = location if location and len(location) else config.GCS_DEFAULT_READ
#   gcs_config.location = loc
#   gcs_config.file_format = loc.split('.')[-1]
#   gcs_config.key_id = secrets["key_id"]
#   gcs_config.secret = secrets['secret']
#   gcs_config.connector_id = "example_input"
#   gcs_conn = GCSConnector(gcs_config)


#   duckdb_conn = duckdb.connect()
#   duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

#   return (gcs_conn, duckdb_conn) 

# def connect_export(conn: dict) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
#   gcs_config = GCSConfiguration()
#   loc_base = conn.get("location", config.GCS_DEFAULT_WRITE)
#   gcs_config.location = loc_base + "/" + "{model}.json"
#   gcs_config.file_format = "json"
#   gcs_config.key_id = conn['keyId']
#   gcs_config.secret = conn['secret']
#   gcs_config.connector_id = "gcs_export"
#   gcs_conn = GCSConnector(gcs_config)


#   duckdb_conn = duckdb.connect()
#   duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

#   return (gcs_conn, duckdb_conn) 