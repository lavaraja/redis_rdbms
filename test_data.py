from local_settings import *
import redis,psycopg2
import random
import datetime
from datetime import datetime, timedelta

conn=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB)

def gen_datetime(min_year=2017, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


## This will genearate some test data to database.
option=input("Do you want to create database tables- yes/no ? : ")
if option=='yes':
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

    with conn:
        with conn.cursor() as cursor:
            cursor.execute("drop table if exists transactions;")
            print("done")
            cursor.execute("drop table if exists  balance;")
            print("done")

            cursor.execute("""create table balance(
                current_balance money,
                user_id varchar(25),
                latest_trn_time timestamp
                )
                ;""")
            cursor.execute("""create table transactions(
                	id varchar(25) unique,
                	trn_identity varchar(25),
                	amount money,
                	trn_details varchar(25),
                	trn_date timestamp
                	);
                """)
            cursor.execute("alter table balance add primary key (user_id );")
            cursor.execute("alter table transactions add foreign key(trn_identity) references balance(user_id) ; ")
        conn.commit()
    conn.close()


elif option=='no':
    print("Not creating database tables . Please create them manually")


option=input("Do you want to load test data to database tables- yes/no ? : ")

if option=='yes':
    user_gen="user"
    trn_gen = "TRX00"
    trn_types=('IMPS','NEFT','CASH','WIRE')
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    for x in range(1,40000):
        with conn.cursor() as cursor:
            userobj=user_gen+str(x)
            trnobj = trn_gen + str(x)
            cursor.execute("update public.balance set current_balance=%d,latest_trn_time=current_timestamp where user_id='%s'" % (random.randint(1,200000),userobj))
            conn.commit()
    conn.close()

