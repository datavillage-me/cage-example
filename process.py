from dv_utils import log, LogLevel, set_event, reset_event

import read_space
import read_bucket
import write_bucket
import read_client


def event_processor(evt: dict):
  """
  Process an incoming event. The `evt` dict has at least the field `type`
  Exception raised by this function are handled by the default event listener and reported in the logs.
  """
  log("event_processor started", LogLevel.INFO)
  if evt["type"] == "EX_READ_SPACE":
    read_space.print_space_info()

  elif evt["type"] == "EX_READ_COLLABORATOR":
    collab_id = evt.get("id", None)
    collab_label = evt.get("label", None)
    read_space.read_collaborator(collab_id=collab_id, collab_label=collab_label)

  elif evt["type"] == "EX_READ_CLIENT_SECRET":
    client_id = evt.get("client_id", None)
    secret_id = evt.get("secret_id", None)
    read_client.read_secret(client_id, secret_id)

  elif evt["type"] == "EX_READ_BUCKET":
    location = evt.get("location", None)
    secret_manager_key = evt.get("secret_manager_key", None)
    read_bucket.read_file(location, secret_manager_key)

  elif evt["type"] == "EX_WRITE_BUCKET":
    location = evt.get("location", None)
    secret_manager_key = evt.get("secret_manager_key", None)
    write_bucket.write_data(evt["data"], location, secret_manager_key)

  elif evt["type"] == "EX_WRITE_BUCKET_SIGNED":
    data = evt['data']
    location = evt.get("location", None)
    secret_manager_key = evt.get("secret_manager_key", None)
    write_bucket.write_signed_data(data, location, secret_manager_key)

  elif evt["type"] == "EX_HYDRATE_CONTRACTS":
    read_bucket.hydrate_contracts()

  log("done processing event", LogLevel.INFO)



def dispatch_event_local(evt: dict):
  """
  Only for local use
  Sets the event that would be set by the listener in the cage
  """
  set_event(evt)
  event_processor(evt)
  reset_event()

if __name__ == "__main__":
  """
  Only for local use
  Test events without a listener or redis queue set up
  """
  evt_read_space = {
    "type": "EX_READ_SPACE"
  }

  evt_read_collaborator = {
    "type": "EX_READ_COLLABORATOR",
    "label": "datavillage_local"
  }

  evt_read_client_secret = {
    "type": "EX_READ_CLIENT_SECRET",
    "client_id": "677e4649eb5dfe5f9737f595",
    "secret_id": "cage-example"
  }

  evt_read_bucket = {
    "type": "EX_READ_BUCKET",
    "location": "gs://cage_example/netflix_titles.csv", # optional
  }

  evt_write_bucket = {
    "type": "EX_WRITE_BUCKET",
    "location": "gs://cage_example", # optional
    "data": {
      "hello": "world",
      "from": "cage"
    }
  }

  evt_write_bucket_signed = {
    "type": "EX_WRITE_BUCKET_SIGNED",
    "location": "gs://cage_example", # optional
    "data": {
      "hello": "world",
      "signed": "data"
    }
  }

  evt_hydrate_contract = {
    "type": "EX_HYDRATE_CONTRACTS"
  }

  # dispatch_event_local(evt_read_space)
  # dispatch_event_local(evt_read_collaborator)
  dispatch_event_local(evt_read_client_secret)
  # dispatch_event_local(evt_read_bucket)
  # dispatch_event_local(evt_write_bucket)
  # dispatch_event_local(evt_write_bucket_signed)
  # dispatch_event_local(evt_hydrate_contract)