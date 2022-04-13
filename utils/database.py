import psycopg2

CONN_INFO = {
    'DB_NAME': 'climatetrace',
    'DB_USER': 'ctraceadmin',
    'DB_PASS':  '9Qr3B3M&PQbj&nTN',
    'DB_HOST':  'rds-climate-trace.watttime.org'
}

def connect( CONN_INFO):
    '''Connect to database with info specified in connection info.
     Current options are 'staging' or 'production'
    Returns psycopg2 cursor'''

    CON_STR = "host='{DB_HOST}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASS}'".format(**CONN_INFO)

    conn = psycopg2.connect(CON_STR)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur