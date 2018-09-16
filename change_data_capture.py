import redis,psycopg2
from local_settings import *
from database_engine import Adaptor
import celery
import sync_up
import sys
import re

def validate_session_key(key):
    redis_conn = redis.StrictRedis(host=RDB_HOST, port=RDB_PORT, db=RDB_DB, password=RDB_PASSWORD)
    rkey=redis_conn.get("session_key")
    print("got ",rkey,"given",key)
    if rkey and key==rkey:
        print("key found")
        return True
    else:
        #print("i am here")
        #("session key required")
        return False

def initiate_cdc(Adaptor,capture_type,key):
    if not validate_session_key(key) or not key:
        print("No valid session key found.Cannot run change data capture on database.")
        sys.exit(1)

    if capture_type=='new':
        redis_conn = redis.StrictRedis(host=RDB_HOST, port=RDB_PORT, db=RDB_DB, password=RDB_PASSWORD)
        cdc_details = redis_conn.hvals('cached_tables')


