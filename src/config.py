import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from the root .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../.env'))

# Database Configuration
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create a single engine instance to be used across the project
engine = create_engine(DATABASE_URL)

# Directory Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data/raw/')
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data/processed/')
OUTPUT_DATA_PATH = os.path.join(BASE_DIR, 'data/outputs/')