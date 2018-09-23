import redis,psycopg2
from local_settings import *
from database_engine import Adaptor
from celery import Celery
import sys
from select_db_engine import *
from sqlalchemy import create_engine
import json,datetime

app = Celery('celerykarman', broker='pyamqp://localhost//', backend='rpc://')

@app.task
def change_capture(table,platform,batch_size):
    redis_client = redis.StrictRedis(host=RDB_HOST, port=RDB_PORT, db=RDB_DB, password=RDB_PASSWORD)
    connection=connection_type(platform)
    engine = create_engine(connection)
    conn = engine.connect()
    try:
        col=json.loads(redis_client.get("tab_details"))[table.split('.')[1]+"_ts"]
        print("ok")
        while (True):
            cached_data=json.loads(redis_client.hget("cached_tables",table))
            last_sync=datetime.datetime.strptime(cached_data["last_cdc_sync"],"%Y-%m-%d %H:%M:%S.%f")
            current=datetime.datetime.now()
            print((current-last_sync).microseconds)
            if ((current-last_sync).microseconds)>50000:
                print("here")
                tabschema=table.split('.')[0]
                tabname=table.split('.')[1]
                setname = table
                res = conn.execute("SELECT COUNT(*) FROM %s.%s where %s >= '%s';" %(tabschema,tabname,col,last_sync))
                tot_rows=res.scalar()
                cached_data["last_cdc_check"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                if tot_rows>0:
                    print("found records to catch",tot_rows)
                    cached_data["last_cdc_sync"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    cached_data["last_cdc_rec_count"] = 0
                    pipline=redis_client.pipeline()
                    rows=conn.execute("SELECT * FROM %s.%s where %s >= '%s';" %(tabschema,tabname,col,last_sync))
                    y=0
                    pipline=redis_client.pipeline()
                    for x in range(0, tot_rows):
                        rec=dict(rows.fetchone())
                        pipline.hset(setname, rec['user_id'], rec)
                        print(rec)
                        if y==batch_size or x==(tot_rows-1):
                            pipline.execute()
                            y=0
                            if x!=tot_rows-1:
                                pipline = redis_client.pipeline()

                        y=y+1;
                else:
                    print(" no records in tables to cache")

                redis_client.hset('cached_tables',table,json.dumps(cached_data))
    except Exception :
        raise(Exception)


    finally:
        conn.close()

@app.task
def test_method(a,b):
    print(a+b)