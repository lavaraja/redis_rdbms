from sqlalchemy import create_engine
from local_settings import *
import sys

def connection_type(platform):
    if platform == 'POSTGRESQL':
        db_engine = 'postgresql'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    elif platform== 'DB2LUW':
        db_engine = 'ibm_db_sa'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    elif platform == 'DB2ZOS':
        db_engine = 'ibm_db_sa'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    elif platform == 'MSSQL':
        db_engine = 'mssql'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    elif platform == 'MYSQL':
        db_engine = 'mysql'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    elif platform == 'ORACLE':
        db_engine = 'oracle + cx_oracle'
        connection_url = db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + ':' + str(DB_PORT) + '/' + DB_NAME

    else:
        raise("Unsupported database Platform type")
    return connection_url
