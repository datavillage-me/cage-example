from dv_utils import SecretManager, audit_log, LogLevel
import json

def decrypt_file(location: str, expected: dict | None):
  files = {'message': open(location, "rb")}
  s = SecretManager()
  resp = s.decrypt(files)
  resp_obj = json.loads(resp)
  if expected:
    equal = json.dumps(resp_obj, sort_keys=True) == json.dumps(expected, sort_keys=True)
    if equal:
      audit_log("decrypted file has expected content", LogLevel.INFO)
    else:
      audit_log(f"comparison failed:\n  Got: {resp_obj}\n  Expected: {expected}", LogLevel.WARN)
      audit_log("decrypted file does not have expected content", LogLevel.INFO)