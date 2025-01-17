import os
import config
import json

def create_tmp_json(data: dict) -> str:
  tmp_file = os.path.join(config.DATA_FOLDER, 'tmp.json')
  with open(tmp_file, 'w') as f:
    f.write(json.dumps(data))
  
  return tmp_file