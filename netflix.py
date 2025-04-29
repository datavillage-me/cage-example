import os
import time
import constants
from io import StringIO, BytesIO
import csv

from dv_utils.log_utils import log, LogLevel
from dv_utils.data_engine import create_client

from dv_data_engine_client.client import Client
from dv_data_engine_client.api.default import mount_collaborator, collaborator_status, query_collaborator, append_collaborator, export_collaborator
from dv_data_engine_client.models.mount_collaborator_body import MountCollaboratorBody
from dv_data_engine_client.models.query_collaborator_body import QueryCollaboratorBody
from dv_data_engine_client.models.append_collaborator_body import AppendCollaboratorBody
from dv_data_engine_client.types import File

def run_netflix_example():
  # step 1: mount/initialize the collaborators
  if not __mount_provider():
    log("could not mount provider. Stopping execution", LogLevel.ERROR)
    return
  
  if not __initialize_consumer():
    log("could not initialize consumer. Stopping execution.", LogLevel.ERROR)
    return
  log("Succesfully initialized collaborators")

  # step 2: peform the query (drop the first line because it is the column names)
  results = __query()[1:]
  log(f"found {len(results)} results")

  # step 3: append results to data consumer
  if not __append_results(results):
    log("could not append results. Stopping execution.", LogLevel.ERROR)
    return
  log("appended results")

  # step 4: export data consumer to bucket
  if not __export_results():
    log("could not export results. Stopping execution", LogLevel.ERROR)
    return
  log("exported results")
  
def __mount_provider() -> bool:
  provider_id = os.environ["ID_NETFLIX_TITLES"]
  with create_client() as c:
    mount_collaborator.sync(client=c, collaborator_id=provider_id, body=MountCollaboratorBody())

    # wait for mounting to be completed
    return __wait_for_status(c, provider_id, "mounted")


def __wait_for_status(client: Client, collab_id: str, expected_status: str) -> bool:
  max_tries = 10
  tries = 0
  sleep_s = 1
  status = __get_collab_status(client, collab_id)
  while expected_status != status and tries < max_tries:
    if status == "error":
      log(f"error for collaborator {collab_id}", LogLevel.ERROR)
      return False
    time.sleep(sleep_s)
    status = __get_collab_status(client, collab_id)
    tries += 1
  
  return status == expected_status

def __get_collab_status(client: Client, collab_id: str) -> str:
  resp = collaborator_status.sync(client=client, collaborator_id=collab_id)
  return resp.to_dict()["status"]  


def __initialize_consumer() -> bool:
  consumer_id = os.environ["ID_EXPORT"]
  body = MountCollaboratorBody.from_dict({"columns": constants.columns})

  with create_client() as c:
    mount_collaborator.sync(client=c, collaborator_id=consumer_id, body=body)
    return __wait_for_status(c, consumer_id, "initialized")
  
def __query() -> list[list[str]]:
  provider_id = os.environ["ID_NETFLIX_TITLES"]
  body = QueryCollaboratorBody.from_dict(constants.query)

  with create_client() as c:
    resp: str = query_collaborator.sync(client=c, collaborator_id=provider_id, body=body)
    reader = csv.reader(StringIO(resp), delimiter=",")
    return [r for r in reader]
  
def __append_results(results: list[list[str]]) -> bool:
  consumer_id = os.environ["ID_EXPORT"] 
  data = StringIO()
  writer = csv.writer(data, quoting=csv.QUOTE_NONNUMERIC)
  writer.writerows(results)

  f = File(payload=BytesIO(data.getvalue().encode()), file_name="data.csv")
  body = AppendCollaboratorBody(data=f)
  
  with create_client() as c:
    append_collaborator.sync(client=c, collaborator_id=consumer_id, body=body)
    return __wait_for_status(c, consumer_id, "mounted")
  
def __export_results() -> bool:
  consumer_id = os.environ["ID_EXPORT"]

  with create_client() as c:
    export_collaborator.sync(client=c, collaborator_id=consumer_id)
    return __wait_for_status(c, consumer_id, "exported")
