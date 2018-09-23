import redis
from sqlalchemy import create_engine
from database_engine import Adaptor
from local_settings import *
import json
import redis
import sys,datetime
redis_client=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB,password=RDB_PASSWORD)
import os
from pathlib import Path
import subprocess as background_shell
from celerykarman import change_capture
import time

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

    if sync_type=='start' :
        check_existing_process();
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
                            y=0
                            if x!=tot_rows-1:
                                pipline = redis_client.pipeline()

                        y=y+1;
                else:

                    print(" no records in tables to cache")

                data = {}
                data['tabname'] = tab
                data['initial_sync_count'] = tot_rows
                data['initial_sync_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                data['last_cdc_check'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                data['last_cdc_sync'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                data['last_cdc_rec_count'] = 0
                json_data = json.dumps(data)
                print(json.dumps(Adaptor.tabdetails))
                #redis_client.set(tab,json.dumps(Adaptor.tabdetails))
                redis_client.hset('cached_tables',tab,json_data)
                print(" table "+tab+" is successfully cached in redis")

            redis_client.set("is_active_session",1)
            redis_client.set('tab_details', json.dumps(Adaptor.tabdetails))
            print("Initiating CDC for all synced tables")
            no_of_tables=len(tables)
            start_celery_worker(tables,4)

            ##call to CDC function.

        except Exception as e:
            print(e)

        finally:
            conn.close()

    elif sync_type=='stop':
        stop_celery_worker()
        
    elif sync_type=='restart':
        print("Currently restart is not supported. Please use stop and start")
        pass
    else:
        print("Unsupported operation")

def start_celery_worker(tables,no_of_workers=4,):
    code = background_shell.call(
        "nohup celery -A celerykarman worker --loglevel=ERROR --pidfile=celery/%n.pid --logfile=celery/%n%I.log >nohup_celery.log &",
        shell=True)
    if code!=0 :
        print("Cannot start the background celery worker.CDC cannot continue.exiting")
        sys.exit(3)
    celery_pid_file = Path("celery/celery.pid")
    print("started celery workers")
    print("verifying..")
    if not celery_pid_file.is_file():
        print("Error verifying Celery worker status")
        sys.exit(4)
    print("Celery worker process found.")
    print("Starting CDC tasks")
    for x in tables:
        change_capture.delay(x,'POSTGRESQL','40000')
    print("Started CDC for all tables.")
    print("Logs are avalible in celery directory")

def check_existing_process():
    celery_pid_file=Path("celery/celery.pid")
    if celery_pid_file.is_file():
        print("existing sync up process found.please stop the process before starting new sync up")
        sys.exit(2)


def stop_celery_worker():
    celery_pid_file=Path("celery/celery.pid")
    if celery_pid_file.is_file():
        with open('celery/celery.pid') as f:
            pid = f.readlines()
        if pid[0]:
            code = background_shell.call("kill -SIGINT "+str(pid),shell=True)
            time.sleep(3)
            code = background_shell.call("kill -SIGINT " + str(pid), shell=True)
            time.sleep(3)
    if not celery_pid_file.is_file():
        print("Stopped sync up process")
    else:
        print("error stopping CDC process")




