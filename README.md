# Redis as front end cache for fast read performace
# Author : Lavaraja padala
# Email : lavaraja.padala@gmail.com
The aim of this project is to test the redis in memory database as front-end cache solution for RDBMS tables.Those tables with high updates/inserts
will be put in redis.The backend job will take care of updating all the records the got updated in the  rdbms table since last read.

We would like improve performance of time critical applications currently facing bottleneck issues due to underlying RDBMS limitations.

Limitations:
Currenlty this tool supports only below tables.
Your table should have primary key and timestamp column.

Usage :



