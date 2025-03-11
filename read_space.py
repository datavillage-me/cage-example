from dv_utils import log, LogLevel
from dv_utils.client import create_client
from control_plane_cage_client.api.collaboration_spaces import get_collaborators
from control_plane_cage_client.api.collaboration_spaces import get_collaborator
from control_plane_cage_client.models.collaborator import Collaborator
import config

def print_space_info():
  collabs = read_collaborators()

  desc = make_description(collabs)
  log(desc, LogLevel.INFO)
  pass

def read_collaborators() -> list[Collaborator]:
  with create_client() as c:
    return get_collaborators.sync(client=c)

def make_description(collaborators: dict) -> str:
  providers, code, consumers = categorize_collaborators(collaborators)
  result = config.DV_CAGE_ID + "\n"
  result += "Data Providers\n"
  for s in providers:
    result += f"  {s}\n"
  
  result += "\n" + "Code Provider\n" + f"  {code}\n"

  result += "\n" + "Data Consumers\n"
  for s in consumers:
    result += f"  {s}\n"
  
  return result


def categorize_collaborators(collaborators: list[Collaborator]) -> tuple[list[str], str, list[str]]:
  providers = []
  consumers = []
  code = None

  for col in collaborators:
    c = col.to_dict()
    if c['role'] == 'DataProvider':
      providers.append(get_label_safe(c))
    elif c['role'] == 'CodeProvider':
      code = get_label_safe(c)
    elif c['role'] == 'DataConsumer':
      consumers.append(get_label_safe(c))

  return (providers, code, consumers)

def get_label_safe(provider: dict) -> str:
  name = provider.get("name", None)
  if name:
    return name
  
  return provider.get("clientId", None)

def read_collaborator(collab_id: str | None, collab_label: str | None):
  if collab_id is None:
    if collab_label is None:
      log("id and label of collab is none", LogLevel.ERROR)
      return
    
    # TODO: get from env
    for col in read_collaborators():
      if col.label == collab_label:
        collab_id = col.id
        break

  collab = None
  with create_client() as c:
    collab = get_collaborator.sync(collaborator_id=collab_id, client=c)
  
  print(f"Got collaborator {collab}")
