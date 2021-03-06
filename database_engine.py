from sqlalchemy import create_engine
from local_settings import *
import sys
import redis
import json

class Adaptor:
    platform = None
    batchsize = 0
    valid = False
    db_engine = None
    tables=[]
    tabdetails = {}

    def __init__(self,platform,batchsize):
        self.platform=platform
        self.batchsize=batchsize
        self.valid=False
        self.db_engine=None
        self.tabdetails={}

    def validate_tables(self,args):
        if self.platform == None:
            raise Exception("Backend RDBMS platform not set")
        for table in args:
            if self.db_table_check(table):
                pass
            else:
                return False
        self.valid = True
        return True


    def db_table_check(self,tablename):
        #print(type(tablename),tablename)
        if self.platform not in ('POSTGRESQL','DB2LUW','DB2ZOS','MSSQL','ORACLE'):
            print("unsupported DBMS platform.Please set platform in localsettings.py")
            sys.exit(1)

        #print(tablename.split('.'))
        #print(len(list(tablename.split('.'))) !=2 )

        tab=tablename.split('.')[1]
        sch=tablename.split('.')[0]

        if not tab and not sch :
            print("Table names should be in schemaname.tablename format",tablename)
            sys.exit(1)

        if self.platform=='POSTGRESQL':
            self.db_engine = 'postgresql'
            connection_url=self.db_engine+'://'+DB_USER+':'+DB_PASSWORD+'@'+DB_HOST+':'+str(DB_PORT)+'/'+DB_NAME
            engine=create_engine(connection_url)
            conn=engine.connect()
            timestamp_pass = False;
            pk_pass = False;
            pk_single_col = False
            try:

                self.tables.append(tablename)
                tab_schema=tablename.split('.')[0]
                tab_name=tablename.split('.')[1]

                result=conn.execute("select column_name,data_type from information_schema.columns where table_schema='%s' and table_name='%s'" %(tab_schema,tab_name))
                for row in result:
                    if 'timestamp' in row['data_type']:
                        self.tabdetails[tab_name+'_ts']=row['column_name']
                        timestamp_pass=True

                        break;
                # This returns the names and data types of all columns of the primary key for the tablename table:
                result1 = conn.execute(
                   "SELECT count(*) as pk_col_count FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_schema='%s' AND  t.table_name = '%s' AND t.constraint_type = 'PRIMARY KEY';" %(tab_schema,tab_name)
                )

                for row in result1:
                    if row['pk_col_count']==1:
                        pk_pass=True
                        pk_single_col=True


                result2 = conn.execute(
                   "SELECT c.column_name, c.ordinal_position FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_schema='%s' AND  t.table_name = '%s' AND t.constraint_type = 'PRIMARY KEY';" %(tab_schema,tab_name)
                )
                for row in result2:
                    self.tabdetails[tab_name + '_pk'] = row['column_name']

                if pk_single_col and pk_pass and timestamp_pass :
                    return True


                else:
                    return False



            except Exception :
                print(Exception)

            finally:
                conn.close()


        elif self.platform=='DB2LUW':
            self.db_engine = 'ibm_db_sa'
            connection_url = self.db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + '/' + DB_NAME

        elif self.platform == 'DB2ZOS':
            self.db_engine = 'ibm_db_sa'
            connection_url = self.db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + '/' + DB_NAME
            print("ok")
        elif self.platform == 'MSSQL':
            self.db_engine = 'mssql'
            connection_url = self.db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + '/' + DB_NAME
            print("ok")
        elif self.platform == 'MYSQL':
            self.db_engine = 'mysql'
            connection_url = self.db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + '/' + DB_NAME
            print("ok")
        elif self.platform == 'ORACLE':
            self.db_engine = 'oracle + cx_oracle'
            connection_url = self.db_engine + '://' + DB_USER + ':' + DB_PASSWORD + '@' + DB_HOST + '/' + DB_NAME
            print("ok")



