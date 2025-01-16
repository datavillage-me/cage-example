from dv_utils import audit_log, LogLevel, set_event

import read_space
import read_bucket
import write_bucket
import decrypt
import os
import config

def event_processor(evt: dict):
  """
  Process an incoming event. The `evt` dict has at least the field `type`
  Exception raised by this function are handled by the default event listener and reported in the logs.
  """
  audit_log("event_processor started", LogLevel.INFO)
  if evt["type"] == "EX_READ_SPACE":
    read_space.print_space_info()

  elif evt["type"] == "EX_READ_BUCKET":
    credentials = evt["credentials"]
    read_bucket.read_file(credentials["keyId"], credentials["secret"])

  elif evt["type"] == "EX_WRITE_BUCKET":
    credentials = evt["credentials"]
    write_bucket.write_data(evt["data"], credentials["keyId"], credentials["secret"])

  elif evt["type"] == "EX_WRITE_BUCKET_SIGNED":
    should_download = evt.get("download", False)
    credentials = evt["credentials"]
    write_bucket.write_signed_data(evt["data"], should_download, credentials["keyId"], credentials["secret"])

  elif evt["type"] == "EX_DECRYPT_FILE":
    expected = evt.get("expected", None)
    decrypt.decrypt_file(evt["path"], expected)

  audit_log("done processing event", LogLevel.INFO)



def dispatch_event_local(evt: dict):
  """
  Only for local use
  Sets the event that would be set by the listener in the cage
  """
  set_event(evt)
  event_processor(evt)

if __name__ == "__main__":
  """
  Only for local use
  Test events without a listener or redis queue set up
  """
  evt_read_space = {
    "type": "EX_READ_SPACE"
  }

  evt_read_bucket = {
    "type": "EX_READ_BUCKET",
    "credentials": {
      "keyId": os.environ['KEY_ID'],
      "secret": os.environ['SECRET']
    }
  }

  evt_write_bucket = {
    "type": "EX_WRITE_BUCKET",
      "credentials": {
      "keyId": os.environ['KEY_ID'],
      "secret": os.environ['SECRET']
    },
    "data": {
      "hello": "world",
      "from": "cage"
    }
  }

  evt_write_bucket_signed = {
    "type": "EX_WRITE_BUCKET_SIGNED",
    "download": True,
    "credentials": {
      "keyId": os.environ['KEY_ID'],
      "secret": os.environ['SECRET']
    },
    "data": {
      "hello": "world",
      "signed": "data"
    }
  }

  evt_decrypt_file = {
    "type": "EX_DECRYPT_FILE",
    "path": os.path.join(config.DATA_FOLDER, "message"),
    "expected": {
      "super": "secret",
      "json": "file"
    }
  }

  # dispatch_event_local(evt_read_space)
  # dispatch_event_local(evt_read_bucket)
  dispatch_event_local(evt_write_bucket)
  # dispatch_event_local(evt_write_bucket_signed)
  # dispatch_event_local(evt_decrypt_file)