import redis,psycopg2
from local_settings import *
from database_engine import Adaptor
import sync_up
import sys
import re
import database_engine
cached_table_instance = Adaptor(DB_PLATFORM, 50000)
f = open("tables_list.txt", "r")
fileString = f.read()
f.close()
processedString = re.sub("\n\s*\n*", "\n", fileString)
f = open("tables_list.txt", "w")
f.write(processedString)
f.close()
with open('tables_list.txt') as f:
    tables = f.read().splitlines()
validated=cached_table_instance.validate_tables(tables)
redis_client=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB,password=RDB_PASSWORD)
redis_client.set("session_key","abc")
redis_client.get("session_key")
sync_up.sync_to_redis(cached_table_instance,"start",redis_client.get("session_key"))