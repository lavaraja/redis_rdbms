from local_settings import *
import redis,psycopg2

conn=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB)
db_conn=psycopg2.connect(database=DB_NAME,user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT)

def sync_balance(id):
    cursor=db_conn.cursor()
    print(conn.hmget('user:balance',id))
    print(id)

    #code
def check_and_sync_balance():
    while True:
        sync_up_list = conn.zrangebyscore('delayset', 10, 50)
        for x in sync_up_list:
            sync_balance(x)
        db_conn.close()





