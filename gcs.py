from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
from dv_utils import log, LogLevel
import duckdb
import secret_manager
   

def connect(location: str, secret_manager_key: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  creds = secret_manager.get_conn_secrets(secret_manager_key)

  gcs_config = GCSConfiguration()
  gcs_config.location = location
  gcs_config.file_format = location.split('.')[-1]
  gcs_config.key_id = creds['key_id']
  gcs_config.secret = creds['secret']
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)
  log("connected to duckdb", LogLevel.INFO)

  return (gcs_conn, duckdb_conn) 
