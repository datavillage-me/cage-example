from dv_utils.connectors.connector import populate_configuration
from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
import config
import duckdb

def connect_gcs(config_file: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  gcs_config = GCSConfiguration()
  populate_configuration(config_file, gcs_config, config.CONNECTOR_FILE_DIR)
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

  return (gcs_conn, duckdb_conn)