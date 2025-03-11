from dv_utils.secrets import get_secret_for_client
from dv_utils.log_utils import log, LogLevel

def read_secret(client_id: str, secret_id: str):
  secret = get_secret_for_client(client_id, secret_id)
  if secret is None:
    log(f"could not get secret {secret_id} for client {client_id}", LogLevel.ERROR)
    return
  
  log(f"got secret of length {len(secret)}", LogLevel.INFO)