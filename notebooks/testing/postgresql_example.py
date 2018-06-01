
# coding: utf-8

# In[3]:

import pandas as pd
import numpy as np
from sqlalchemy import *
import datetime


DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME = "y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v01"

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

sql = "SELECT * FROM {}        LIMIT 10".format(TABLE_NAME)

df = pd.read_sql(sql, connection)


# ## Some Background
# 
# the database is the result of running zonal statistics on a climate model. I am relatively new to postgreSQL and haven't set any indexes yet. The database is on AWS RDS on an x.large instance. None of the columns is unique. "pfafid_30spfaf06" is a zonal code for water basins. There are in total appr. 16000 unique pfaf_ids. year [1960-2014] month [1-12], temporal_resolution ["year","month"]
# 
# Result of (PgAdmin) :   
# `SELECT pg_size_pretty(pg_total_relation_size('"public"."y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v01"'));`
# 
# Successfully run. Total query runtime: 425 msec.
# 1 rows affected:
# 
# 11 GB
# 
# 
# Result of (PgAdmin) :  
# `SELECT count(*) FROM y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v01`
# 
# Successfully run. Total query runtime: 52 secs.
# 1 rows affected.
# 
# 11715275  i.e. 11,715,275 rows
# 

# In[4]:

df.head()


# I know combining month and year in one datetime column is best practice but for future use, keeping them separate is easier.

# The query I like to run calculates a 10 year moving average for three columns:  
# 1. ptotwn_m_30spfaf06
# 1. ptotww_m_30spfaf06
# 1. riverdischarge_m_30spfaf06
# 
# For axample the 10y annual moving average of 1969 is the average of 1960 - 1969. For a monthly moving average the average is filtered by month: average of jan 1960 jan 1961 ... jan 1969. 
# 
# The query I have so far:
# 
# `
# SELECT year, ptotww_m_30spfaf06, temporal_resolution,
#   SUM(ptotww_m_30spfaf06)
#     OVER(ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ptotwwma_m_30spfaf06 
# FROM y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v01
# WHERE temporal_resolution = 'year'
# LIMIT 200`
# 
# However this is slow (need to set index? Which columns? year, month?) and does not work for the monthly scores. 
# 
# Successfully run. Total query runtime: 52 secs.
# 200 rows affected.
# 
# Which is quite slow. 

# In[ ]:



