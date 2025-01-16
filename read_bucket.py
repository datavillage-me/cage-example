from dv_utils import audit_log, LogLevel
import config
import gcs

def read_file():
  gcs_conn, duckdb_conn = gcs.connect_gcs(config.INPUT_CONNECTOR)

  from_statement = gcs_conn.get_duckdb_source()
  result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()
  audit_log(f"found {result['count'][0]} rows", LogLevel.INFO)