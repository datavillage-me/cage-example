import json
import config
from dv_utils import log, LogLevel
import requests

def get_conn_secrets(secret_manager_key: str = None) -> dict:
  key = secret_manager_key if secret_manager_key and len(secret_manager_key) else config.SECRET_MANAGER_KEY
  log(f"downloading secrets {key}", LogLevel.INFO)
  
  resp = requests.get(f"{config.SECRET_MANAGER_URL}/secrets/{key}?plaintext=true")
  if resp.status_code != 200:
    log(f"unexpected status code returned by secret manager: [{resp.status_code}]: {resp.text}", LogLevel.ERROR)
    return {}
  
  try:
    return json.loads(resp.text)
  except Exception as e:
    log(f"could not parse secret manager response to json: {str(e)}", LogLevel.ERROR)
    return {}