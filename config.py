import os

DATA_FOLDER="data/input"

ALGO_VERSION = '0.0.1'
DV_TOKEN = os.environ['DV_TOKEN']
CONTROL_PLANE_URL = os.environ['CONTROL_PLANE_URL']
DV_CAGE_ID = os.environ['DV_CAGE_ID']

INPUT_CONNECTOR="gcs_netflix"
OUTPUT_CONNECTOR="gcs_export"
CONNECTOR_FILE_DIR="scrapbook"