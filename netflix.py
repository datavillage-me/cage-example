import os
import time
import constants

from dv_utils.log_utils import log, LogLevel
from dv_utils.data_engine import create_client

from dv_data_engine_client.client import Client
from dv_data_engine_client.api.default import mount_collaborator, collaborator_status
from dv_data_engine_client.models.mount_collaborator_body import MountCollaboratorBody

def run_netflix_example():
  if not __mount_provider():
    log("could not mount provider, stopping execution", LogLevel.ERROR)
    return
  
  if not __initialize_consumer():
    log("could not initialize consumer, stopping execution.", LogLevel.ERROR)
    return

  print("collaborators mounted")
  
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