from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
from dv_utils import log, LogLevel
import duckdb
import secret_manager
   

def connect(location: str, secret_manager_key: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  creds = secret_manager.get_conn_secrets(secret_manager_key)

  gcs_config = GCSConfiguration()
  gcs_config.location = location
  gcs_config.file_format = location.split('.')[-1]
  gcs_config.key_id = creds['server']['key_id']
  gcs_config.secret = creds['server']['secret']
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)
  log("connected to duckdb", LogLevel.INFO)

  return (gcs_conn, duckdb_conn) 

def connect_collorator(c_dict: dict) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  key = c_dict.get("label", c_dict.get("id", None))
  secret = secret_manager.get_conn_secrets(f"configuration_{key}")
  server_config = secret.get("server", dict())

  if server_config.get("type", None) != "custom" or server_config.get("custom_type", None) != "gcs":
    log("collaborator config is not GCS", LogLevel.ERROR)
    return (None, None)
  
  contract_id = secret.get("dataContract", None)
  if not contract_id or contract_id != c_dict['dataContract']:
    log("datacontract id in secret doesn't match id in collaborator", LogLevel.ERROR)
    return (None, None)

  gcs_config = GCSConfiguration()
  
  
  location = __build_location(server_config)
  if not location:
    log("could not get location from secret", LogLevel.ERROR)
    return (None, None)
  
  gcs_config.location = location
  gcs_config.file_format = server_config.get("format", "").split(".")[-1]
  gcs_config.key_id = server_config.get("key_id", None)
  gcs_config.secret = server_config.get("secret", None)
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)
  log("connected to duckdb", LogLevel.INFO)

  return (gcs_conn, duckdb_conn)  

def __build_location(server_config: dict) -> str:
  loc: str = server_config.get("location", None)
  if not loc:
    return None

  if not loc.endswith("/"):
    loc = loc + '/'

  path = server_config.get("path", None)
  if path:
    loc = f"{loc}{path}"
  
  return loc