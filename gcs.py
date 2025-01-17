from dv_utils.connectors.gcs import GCSConfiguration, GCSConnector
import duckdb

def connect_import(key_id: str, secret: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  gcs_config = GCSConfiguration()
  gcs_config.location = "gs://cage_example/netflix_titles.csv"
  gcs_config.file_format = "csv"
  gcs_config.key_id = key_id
  gcs_config.secret = secret
  gcs_config.connector_id = "gcs_netflix"
  gcs_conn = GCSConnector(gcs_config)


  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

  return (gcs_conn, duckdb_conn) 

def connect_export(key_id: str, secret: str) -> tuple[GCSConnector, duckdb.duckdb.DuckDBPyConnection]:
  gcs_config = GCSConfiguration()
  gcs_config.location = "gs://cage_example/{model}.json"
  gcs_config.file_format = "json"
  gcs_config.key_id = key_id
  gcs_config.secret = secret
  gcs_config.connector_id = "gcs_export"
  gcs_conn = GCSConnector(gcs_config)


  duckdb_conn = duckdb.connect()
  duckdb_conn = gcs_conn.add_duck_db_connection(duckdb_conn)

  return (gcs_conn, duckdb_conn) 