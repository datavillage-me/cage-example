import os

# DATA_FOLDER="data/input"
DATA_FOLDER="/resources/data"
os.makedirs(DATA_FOLDER, exist_ok=True)

ALGO_VERSION = '0.6.0rc3'
CONTROL_PLANE_URL = os.environ['CONTROL_PLANE_URL']
DV_CAGE_ID = os.environ['DV_CAGE_ID']

SECRET_MANAGER_KEY = "configuration_datavillage"
SECRET_MANAGER_URL = os.environ['SECRET_MANAGER_URL']
DV_TOKEN = os.environ.get("DV_TOKEN", "")

GCS_DEFAULT_READ = "gs://cage_example/netflix_titles.csv"
GCS_DEFAULT_WRITE = "gs://cage_example"
