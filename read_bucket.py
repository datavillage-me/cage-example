from dv_utils import audit_log, LogLevel
import gcs
import time
import config

def read_file(location: str = None, secret_manager_key: str = None, retry = True):
  try:
    loc = location if location and len(location) else config.GCS_DEFAULT_READ
    s_key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY
    gcs_conn, duckdb_conn = gcs.connect(loc, s_key)

    from_statement = gcs_conn.get_duckdb_source(options="")

    result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()

    count = result['count'][0]
    audit_log(f"found {count} rows", LogLevel.INFO)
  except Exception as e:
    if retry:
      audit_log("got error while reading file from GCS. retrying...", LogLevel.WARN)
      time.sleep(1)
      return read_file(location, secret_manager_key, False)
    else:
      audit_log(f"could not read file: {e}", LogLevel.ERROR)
      