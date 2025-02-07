import config
import requests
from dv_utils import log, LogLevel

def print_space_info():
  endpoint = f"{config.CONTROL_PLANE_URL}/collaboration-spaces/{config.DV_CAGE_ID}"
  token = config.DV_TOKEN
  response = requests.get(endpoint, headers={'Authorization': f"Bearer {token}"})
  if not response.ok:
    log(f"could not get collaboration space. Got [{response.status_code}]: {response.text}", LogLevel.ERROR)
    return
  
  space = response.json()
  desc = make_description(space)
  log(desc, LogLevel.INFO)
  pass

def make_description(space: dict) -> str:
  providers, code, consumers = categorize_collaborators(space)
  result = space['name'] + "\n"
  result += "Data Providers\n"
  for s in providers:
    result += f"  {s}\n"
  
  result += "\n" + "Code Provider\n" + f"  {code}\n"

  result += "\n" + "Data Consumers\n"
  for s in consumers:
    result += f"  {s}\n"
  
  return result


def categorize_collaborators(space: dict) -> tuple[list[str], str, list[str]]:
  providers = []
  consumers = []
  code = None

  for c in space['collaborators']:
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
