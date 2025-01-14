from dv_utils import audit_log, LogLevel, set_event

import read_space
import read_bucket
import write_bucket

def event_processor(evt: dict):
  """
  Process an incoming event. The `evt` dict has at least the field `type`
  Exception raised by this function are handled by the default event listener and reported in the logs.
  """
  audit_log("event_processor started", LogLevel.INFO)
  if evt["type"] == "EX_READ_SPACE":
    read_space.print_space_info()
  elif evt["type"] == "EX_READ_BUCKET":
    read_bucket.read_file()
  elif evt["type"] == "EX_WRITE_BUCKET":
    write_bucket.write_data(evt["data"])
  elif evt["type"] == "EX_WRITE_BUCKET_SIGNED":
    write_bucket.write_signed_data(evt["data"])

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
    "type": "EX_READ_BUCKET"
  }

  evt_write_bucket = {
    "type": "EX_WRITE_BUCKET",
    "data": {
      "hello": "world",
      "from": "cage"
    }
  }

  evt_write_bucket_signed = {
    "type": "EX_WRITE_BUCKET_SIGNED",
    "data": {
      "hello": "world",
      "signed": "data"
    }
  }

  # dispatch_event_local(evt_read_space)
  # dispatch_event_local(evt_read_bucket)
  # dispatch_event_local(evt_write_bucket)
  dispatch_event_local(evt_write_bucket_signed)