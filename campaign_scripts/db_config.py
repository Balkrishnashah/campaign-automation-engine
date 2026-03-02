import psycopg2


#Define DB Variables

DBNAME = "campaign_db"
USER = "balkrishna"
PASSWORD = "postgressql@14"
HOST = "localhost"

conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    password=PASSWORD,
    host=HOST
)

