from dv_utils.connectors.connector import populate_configuration
from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
from dv_utils import audit_log, LogLevel
import config
import duckdb

def read_file():
  gcs_config = GCSConfiguration()
  populate_configuration(config.CONNECTOR_FILE_NAME, gcs_config, config.CONNECTOR_FILE_DIR)
  gcs_conn = GCSConnector(gcs_config)

  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

  from_statement = gcs_conn.get_duckdb_source()
  result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()
  audit_log(f"found {result['count'][0]} rows", LogLevel.INFO)