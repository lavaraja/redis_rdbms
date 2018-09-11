import redis
from sqlalchemy import create_engine
from database_engine import Adaptor
from local_settings import *
import json
import redis
redis_client=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB,password=RDB_PASSWORD)

def sync_to_redis(Adaptor,sync_type,key):
    print(Adaptor.tabdetails)
    if sync_type=='start':
        redis_reset=input("Do you want clear existing keys in redis.  yes/no ? :" )
        if redis_reset=='yes':
            if confirm_no_active_instance_is_running():
                redis_client.flushall()


    elif sync_type=='stop':
        print("Currently stop is not supported")
        pass
    elif sync_type=='restart':
        print("Currently restart is not supported")
        pass

def start_initial_tab_sync(Adaptor):


    return True

def confirm_no_active_instance_is_running():
    return True

def validate_session_key(key):
    redis_conn = redis.StrictRedis(host=RDB_HOST, port=RDB_PORT, db=RDB_DB, password=RDB_PASSWORD)
    rkey=redis_conn.get("session_key")
    if rkey and key==rkey:
        print("session key required")
        return True
    else:
        return False