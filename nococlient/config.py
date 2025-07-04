import os
from dotenv import load_dotenv

load_dotenv()

NOCO_URL = os.getenv("NOCO_URL", "http://localhost:8080")
NC_AUTH_JWT_SECRET = os.getenv("NC_AUTH_JWT_SECRET", "default-secret")

