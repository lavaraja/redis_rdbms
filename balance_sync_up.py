from local_settings import *
import redis,psycopg2,datetime

conn=redis.StrictRedis(host=RDB_HOST,port=RDB_PORT,db=RDB_DB,password=RDB_PASSWORD)
db_conn=psycopg2.connect(database=DB_NAME,user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT)

def ingest_in_cache(table):
    ##check for table pK and timestamp columns.
    # rdbms cursor
    #open redis pipeline use multiple pipelines if no.of records are more.
    print('starting time :',datetime.datetime.now())
    pline=conn.pipeline()
    for x in range(0,1000000):#range should be the no.of records in rdbms table
        #cursor.fetch
        #convert record to json and encrypt
        #set name can be tablename
        setname='abc.table1'
        key_name='userwer_'+str(x)
        data='ewogICAgIl9pZCI6ICI1YjkxMzFjMzM1MTZiYTUzMjM4MjNlNGMiLAogICAgImluZGV4IjogMCwK\nICAgICJndWlkIjogIjYzYzk1NDFhLWQwYjgtNDgzMC1iNDhlLTM1MTE5NjgwOWQ4NSIsCiAgICAi\naXNBY3RpdmUiOiBmYWxzZSwKICAgICJiYWxhbmNlIjogIiQxLDMzMC4xMSIsCiAgICAicGljdHVy\nZSI6ICJodHRwOi8vcGxhY2Vob2xkLml0LzMyeDMyIiwKICAgICJhZ2UiOiAyMCwKICAgICJleWVD\nb2xvciI6ICJicm93biIsCiAgICAibmFtZSI6ICJBbGV4YW5kcmlhIFdhbGtlciIsCiAgICAiZ2Vu\nZGVyIjogImZlbWFsZSIsCiAgICAiY29tcGFueSI6ICJQTEFTTU9TSVMiLAogICAgImVtYWlsIjog\nImFsZXhhbmRyaWF3YWxrZXJAeHl6LmNvbSIsCiAgICAicGhvbmUiOiAiKzEgKDk2NykgNDkyLTMy\nMzMiLAogICAgImFkZHJlc3MiOiAiNjYwIERvb25lIENvdXJ0LCBBYmlxdWl1LCBXaXNjb25zaW4s\nIDU1NTAiLAogICAgImFib3V0IjogIkVpdXNtb2Qgbm9uIHJlcHJlaGVuZGVyaXQgY29uc2VxdWF0\nIGxhYm9yZSBsYWJvcnVtIGFkaXBpc2ljaW5nIGlydXJlIGN1cGlkYXRhdCBlaXVzbW9kIG9mZmlj\naWEgYWxpcXVpcCBldCBpcnVyZS4gRWxpdCBhbmltIGxhYm9ydW0gbW9sbGl0IGRvbG9yZS4gRXN0\nIG51bGxhIHJlcHJlaGVuZGVyaXQgaXBzdW0gbW9sbGl0IHByb2lkZW50IHNpdCBlYS4gU2ludCBk\ndWlzIGVpdXNtb2QgYWRpcGlzaWNpbmcgZXUgbmlzaSBleGVyY2l0YXRpb24gbGFib3JlIGluIGlw\nc3VtIGlkLiBWb2x1cHRhdGUgY29uc2VxdWF0IGluIGFsaXF1aXAgZWEgbGFib3JpcyB0ZW1wb3Ig\nZG9sb3IgdGVtcG9yIG9jY2FlY2F0IGFkIG1hZ25hIG51bGxhIGlwc3VtLiBOb24gY29uc2VjdGV0\ndXIgY29tbW9kbyBub3N0cnVkIGZ1Z2lhdCB1dCBkbyB1dCBkZXNlcnVudC4gTGFib3JlIG5vc3Ry\ndWQgdGVtcG9yIGxhYm9ydW0gY3VscGEgdWxsYW1jbyBMb3JlbSBxdWkgdmVuaWFtIG5pc2kuDQoi\nLAogICAgInJlZ2lzdGVyZWQiOiAiMjAxNC0wNC0yOFQwNjo0MzozMSAtMDY6LTMwIiwKICAgICJs\nYXRpdHVkZSI6IDI4Ljc2MzY2NCwKICAgICJsb25naXR1ZGUiOiAxNzAuNzA1MjQyLAogICAgInRh\nZ3MiOiAgICAgICAiZXNzZSIsICAKICAgICJncmVldGluZyI6ICJIZWxsbywgQWxleGFuZHJpYSBX\nYWxrZXIhIFlvdSBoYXZlIDEwIHVucmVhZCBtZXNzYWdlcy4iLAogICAgImZhdm9yaXRlRnJ1aXQi\nOiAiYXBwbGUiCg==\n'
        pline.hset(setname,key_name,data)
    print('pipeline time :', datetime.datetime.now())
    pline.execute()
    print('execute pipeline time :', datetime.datetime.now())

ingest_in_cache('test')




