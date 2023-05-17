import os
from dotenv import load_dotenv


load_dotenv()

## postgres connection ; fill in your own credentials
PG_USER = os.getenv("POSTGRES_USER")
PG_PWD = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_DB = os.getenv("POSTGRES_DB")
PG_PORT = os.getenv("POSTGRES_PORT")
