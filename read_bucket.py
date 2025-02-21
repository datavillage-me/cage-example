from dv_utils import log, LogLevel
from dv_utils.connectors.gcs import GCSConnector
from dv_utils.client import create_client
from control_plane_cage_client.api.data_contracts import get_data_contract
from control_plane_cage_client.models.data_contract import DataContract
import gcs
import time
import config
import read_space
import duckdb

def read_file(location: str = None, secret_manager_key: str = None, retry = True):
  try:
    loc = location if location and len(location) else config.GCS_DEFAULT_READ
    s_key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY
    gcs_conn, duckdb_conn = gcs.connect(loc, s_key)

    from_statement = gcs_conn.get_duckdb_source(options="")

    result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()

    count = result['count'][0]
    log(f"found {count} rows", LogLevel.INFO)
  except Exception as e:
    if retry:
      log("got error while reading file from GCS. retrying...", LogLevel.WARN)
      time.sleep(1)
      return read_file(location, secret_manager_key, False)
    else:
      log(f"could not read file: {e}", LogLevel.ERROR)

def print_file(connector: GCSConnector, duckdb_conn: duckdb.duckdb.DuckDBPyConnection):
  try:
    from_statement = connector.get_duckdb_source(options="")

    result = duckdb_conn.sql(f"SELECT COUNT(*) as count FROM {from_statement}").df()

    count = result['count'][0]
    log(f"found {count} rows", LogLevel.INFO)
  except Exception as e:
    log(f"could not read file: {e}", LogLevel.ERROR)

def hydrate_contracts():
  collabs = read_space.read_collaborators()

  for c in collabs:
    c_dict = c.to_dict()
    if c_dict["role"] == "DataProvider" and "dataContract" in c_dict:
      __hydrate_contract(c_dict)



def __hydrate_contract(c_dict: dict) -> dict:
  connector, duckdb_conn = gcs.connect_collorator(c_dict)
  contract: DataContract = None
  with create_client() as c:
    contract = get_data_contract.sync(client=c, contract_id=c_dict["dataContract"])
  
  if contract:
    col_id = c_dict.get("label", c_dict["id"])
    con_id = contract.data_contract.name
    log(f"Collaborator {col_id} provides contract '{con_id}'", LogLevel.INFO)

  print_file(connector, duckdb_conn)
      