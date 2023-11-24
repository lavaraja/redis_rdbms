# Redis as front end cache for fast read performance

The aim of this project is to test the redis in memory database as front-end cache solution for RDBMS tables.Those tables with high updates/inserts
will be put in redis.The backend job will take care of updating all the records the got updated in the  rdbms table since last read.

We would like improve performance of time critical applications currently facing bottleneck issues due to underlying RDBMS limitations.

Limitations:
Tool supports only tables with timestamp column. This column is used for capturing updated rows.
Table should have primary key and timestamp column.


Usage :

The code is developed using Python-3.Although it should work with python2 but I have not tested this code in Python2.

Install dependencies using python pip.

$ pip install -r requirements.txt

Steps:

tables_list.txt - This file will contain the list of tables that needs to be cached in rdbms.Add tables names with [schema_name].[tablename]  in the file.

local_settings will have all database configuration values for redis and rdbms(PostgreSQL,MySQL,Oracle,Mssql,DB2). Currently only PostgreSQL is supported.


Usage:

./accelerator.py start  == > this will load tables into redis cache . Once the tables are loaded backend jobs will be started.These jobs will capture

any changes done to source table in RDBMS and and update corresponding records in redis. Loading RDBMS table to redis is very fast due to its in memory

storage and pipelining concepts in redis.

./accelerator.py stop == > to stop already running instance.



Any suggestions and feedback is welcome. Contact me at lavaraja.padala@gmail.com .
