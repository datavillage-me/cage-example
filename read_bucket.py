from dv_utils import audit_log, LogLevel
import gcs
import time

def read_file(conn: dict, retry = True):
  try:
    gcs_conn, duckdb_conn = gcs.connect_import(conn)

    from_statement = gcs_conn.get_duckdb_source(options="")

    result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()
    count = result['count'][0]
    audit_log(f"found {count} rows", LogLevel.INFO)
  except Exception as e:
    if retry:
      audit_log("got error while reading file from GCS. retrying...", LogLevel.WARN)
      time.sleep(1)
      read_file(conn, False)
    else:
      audit_log(f"could not read file: {e}", LogLevel.ERROR)
      