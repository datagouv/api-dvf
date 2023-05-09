import os

load_dotenv()

## postgres connection ; fill in your own credentials
PG_ID = os.getenv("POSTGRES_ID")
PG_PWD = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_DB = os.getenv("POSTGRES_DB")
PG_PORT = os.getenv("POSTGRES_PORT")
