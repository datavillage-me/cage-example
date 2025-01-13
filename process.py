from dv_utils import audit_log, LogLevel, set_event

def event_processor(evt: dict):
  """
  Process an incoming event. The `evt` dict has at least the field `type`
  Exception raised by this function are handled by the default event listener and reported in the logs.
  """
  audit_log("event_processor started", LogLevel.INFO)
  pass


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
  evt = {
    "type": "test",
    "hello": "world"
  }
  dispatch_event_local(evt)