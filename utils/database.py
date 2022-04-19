import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import types
import pandas as pd
from geoalchemy2 import types as gtypes

gtypes.Geometry

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
    if len(df.columns) != 38:
        ermin_spec = pd.read_csv('../templates/ermin-specification.csv')
        columns = ermin_spec['Structured name']
        empty_ermin_df = pd.DataFrame(columns=columns)
        empty_ermin_df[df.columns] = df

    empty_ermin_df['reporting_timestamp'] = pd.to_datetime(empty_ermin_df['reporting_timestamp'])
    empty_ermin_df['start_time'] = pd.to_datetime(empty_ermin_df['start_time'])
    empty_ermin_df['end_time'] = pd.to_datetime(empty_ermin_df['end_time'])

    engine = connect(CONN_INFO)

    empty_ermin_df.to_sql('ermin',
              engine,
              if_exists='append',
              index=False)
