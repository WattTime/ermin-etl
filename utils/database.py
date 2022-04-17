import psycopg2
from sqlalchemy import create_engine
import pandas as pd
CONN_INFO = {
    'DB_NAME': 'climatetrace',
    'DB_USER': 'ctraceadmin',
    'DB_PASS':  '9Qr3B3M&PQbj&nTN',
    'DB_HOST':  'rds-climate-trace.watttime.org'
}


def connect(CONN_INFO):
    '''Connect to database with info specified in connection info.
     Current options are 'staging' or 'production'
    Returns psycopg2 cursor'''

    db_connection_url = "postgresql+psycopg2://{}:{}@{}/{}".format(
        CONN_INFO['DB_USER'],
        CONN_INFO['DB_PASS'],
        CONN_INFO['DB_HOST'],
        CONN_INFO['DB_NAME']
    )

    engine = create_engine(db_connection_url)

    return engine


def insert_clean_data(df):
    engine = connect(CONN_INFO)
    df.to_sql('ermin',
              engine,
              if_exists='append',
              index=False )
