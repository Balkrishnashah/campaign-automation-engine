import psycopg2
from sqlalchemy import create_engine


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


# creating an egnine instead of using conn connection so that sqlalchemy should not 
# throw warnings fucking each time, you can us above conn connection too instead of that


engine = create_engine(
    "postgresql+psycopg2://",
    connect_args={
        "dbname": DBNAME,
        "user": USER,
        "password": PASSWORD,
        "host": HOST
    }
)