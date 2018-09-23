import redis,psycopg2
from local_settings import *
from database_engine import Adaptor
import sync_up
import sys
import re,json

script_name=sys.argv[0]
args=sys.argv[1:]

if not args:
    help="""
         please start the script with below arguments
         ./accelerator.py start   # to start the accelerator.py
         ./accelerator.py resume  # to resume the already stopped process.
         ./accelerator.py stop    # to stop the script an running script
    
    """
    print(help)

elif args[0] in ('stop','start','restart'):
    print("Checking for the list of tables")
    try:
        f = open("tables_list.txt", "r")
        fileString = f.read()
        f.close()
        processedString = re.sub("\n\s*\n*", "\n", fileString)
        f = open("tables_list.txt", "w")
        f.write(processedString)
        f.close()
        with open('tables_list.txt') as f:
            tables = f.read().splitlines()
    except:
        print("table_list.txt not  found in the current path")
    print("Below tables were choosen for caching :",tables)

    print()
    print("Validating tables....\n")
    cached_table_instance = Adaptor(DB_PLATFORM, 50000)
    print(tables)
    validated=cached_table_instance.validate_tables(tables)
    if not validated:
        print("Tables validation failed.")
        sys.exit(1)
    else:
        print("Tables are valid")
        redis_client = redis.StrictRedis(host=RDB_HOST, port=RDB_PORT, db=RDB_DB, password=RDB_PASSWORD)

        redis_client.set('tab_details',json.dumps(cached_table_instance.tabdetails))
        print("data")
        redis_client.set("session_key", "abc")
        if args[0]=="start":
            sync_up.sync_to_redis(cached_table_instance, "start", redis_client.get("session_key"))
        elif args[0]=="stop":
            sync_up.sync_to_redis(cached_table_instance, "stop", redis_client.get("session_key"))
        elif args[0]=="restart":
            sync_up.sync_to_redis(cached_table_instance, "restart", redis_client.get("session_key"))


else:
    print("invalid option.")
    print(help)


#cached_table_instance=Adaptor('POSTGRESQL',0)
#tab_list=['public.balance','public.transactions']
#print(cached_table_instance.validate_tables(tab_list))
#print(cached_table_instance.valid)

