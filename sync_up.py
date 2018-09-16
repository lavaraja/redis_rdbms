import redis
from sqlalchemy import create_engine
from database_engine import Adaptor
from local_settings import *
import json
import redis
import sys,datetime
redis_client=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB,password=RDB_PASSWORD)


def confirm_no_active_instance_is_running():
    return True

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

def sync_to_redis(Adaptor,sync_type,key):
    #print(Adaptor.tabdetails)
    if not validate_session_key(key):
        raise("No valid session key found")
        #sys.exit(1)

    if sync_type=='start' and confirm_no_active_instance_is_running():
        redis_reset=input("Do you want to clear existing keys in redis.  yes/no ? :" )
        if redis_reset=='yes':
            if confirm_no_active_instance_is_running():
                redis_client.flushall()
                print("redis db reset complete")
        connection_url = Adaptor.db_engine +'://'+DB_USER+':'+DB_PASSWORD+'@'+DB_HOST+':'+str(DB_PORT)+'/'+DB_NAME
        try:
            engine=create_engine(connection_url)
            global conn
            conn = engine.connect()
            tables=Adaptor.tables
            batch_size=Adaptor.batchsize
            for tab in tables:
                tabschema=tab.split('.')[0]
                tabname=tab.split('.')[1]
                setname = tab
                res = conn.execute("SELECT COUNT(*) FROM %s.%s;" %(tabschema,tabname))
                tot_rows=res.scalar()
                if tot_rows>0:
                    pipline=redis_client.pipeline()
                    rows=conn.execute("SELECT * FROM %s.%s;" %(tabschema,tabname))
                    y=0
                    pipline=redis_client.pipeline()
                    for x in range(0, tot_rows):
                        rec=dict(rows.fetchone())
                        pipline.hset(setname, rec['user_id'], rec)
                        if y==batch_size or x==(tot_rows-1):
                            pipline.execute()
                            if x!=tot_rows-1:
                                pipline = redis_client.pipeline()

                        y=y+1;
                else:

                    print(" no records in tables to cache")

                cached_tab=dict({
                    'tabname':tab,
                    'initial_sync_count':tot_rows,
                    'initial_sync_time':datetime.datetime.now(),
                    'last_cdc_check': datetime.datetime.now(),
                    'last_cdc_sync':datetime.datetime.now(),
                    'last_cdc_rec_count':0

                })

                redis_client.hset('cached_tables',tab,cached_tab)
                print(" table "+tab+" is successfully cached in redis")

        except Exception as e:
            print(e)

        finally:
            conn.close()

    elif sync_type=='stop':
        print("Currently stop is not supported")
        pass
    elif sync_type=='restart':
        print("Currently restart is not supported")
        pass
    else:
        print("Existing running session found. Please stop the session before stating new session")

