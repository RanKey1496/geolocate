import os
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_elastic_url():
    return os.environ.get("ELASTIC_URL")

def get_elastic_key():
    return os.environ.get("ELASTIC_KEY")