import os

DATA_FOLDER="data/input"

ALGO_VERSION = '0.4.0'
DV_TOKEN = os.environ['DV_TOKEN']
CONTROL_PLANE_URL = os.environ['CONTROL_PLANE_URL']
DV_CAGE_ID = os.environ['DV_CAGE_ID']
SECRET_MANAGER_URL = os.environ['SECRET_MANAGER_URL']

INPUT_CONNECTOR="gcs_netflix"
OUTPUT_CONNECTOR="gcs_export"
CONNECTOR_FILE_DIR="scrapbook"