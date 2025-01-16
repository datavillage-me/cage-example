from dv_utils import audit_log, LogLevel
import gcs

def read_file(key_id: str, secret: str):
  gcs_conn, duckdb_conn = gcs.connect_import(key_id, secret)

  from_statement = gcs_conn.get_duckdb_source(options="")

  result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()
  count = result['count'][0]
  audit_log(f"found {count} rows", LogLevel.INFO)