
# coding: utf-8

# In[1]:

""" QA for water stress in several basin
-------------------------------------------------------------------------------

Create postGIS table for selected basins with all ma_10 indicators




Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D04_RH_QA_ma10_results_PostGIS_V03'
OUTPUT_VERSION = 4

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

SIMPLIFY_COLUMN_NAMES = 1 # ForS Shapefile and ArcGIS use.

INPUT_TABLE_LEFT_NAME = 'y2018m06d04_rh_water_stress_postgis_30spfaf06_v01_v03'
INPUT_TABLE_RIGHT_NAME = 'hybas06_v04'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA = "test"

DICT = {}
"""
DICT["basin_172111"] =  {"TEMPORAL_RESOLUTION":None,  # Option [ "year", "month"]
                         "YEAR_RANGE":[1960,2014], # Options [1960:2014]
                         "MONTH_RANGE":[1,12], # Options [1:12]
                         "PFAFID_RANGE":[172111,172111]} # Options, list [111011:914900], includes 0 and end.

DICT["global_year_2014"] =  {"TEMPORAL_RESOLUTION":"year",  # Option [ "year", "month"]
                             "YEAR_RANGE":[2014,2014], # Options [1960:2014]
                             "MONTH_RANGE":[1,12], # Options [1:12]
                             "PFAFID_RANGE":[111011,914900]} # Options, list [111011:914900], includes 0 and end.

"""

DICT["spain_2010"] =  {"TEMPORAL_RESOLUTION":None,  # Option [ "year", "month"]
                       "YEAR_RANGE":[2010,2010], # Options [1960:2014]
                       "MONTH_RANGE":[1,12], # Options [1:12]
                       "PFAFID_RANGE":[230000,240000]} # Options, list [111011:914900], includes 0 and end.


print("Input Table Left: " , INPUT_TABLE_LEFT_NAME, 
      "\nInput Table Right: " , INPUT_TABLE_RIGHT_NAME, 
      "\nOutput Table: " , OUTPUT_SCHEMA +"."+OUTPUT_TABLE_NAME)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import re
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[ ]:




# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()


def create_sample_database():
    sqls = []

    if OVERWRITE_OUTPUT:
        sqls.append("DROP TABLE IF EXISTS {}.{};".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME))
        sqls.append("DROP TABLE IF EXISTS {}.temp_left".format(OUTPUT_SCHEMA))



    sql = ""
    sql += "CREATE TABLE {}.temp_left AS ".format(OUTPUT_SCHEMA)
    sql += "SELECT * FROM {} ".format(INPUT_TABLE_LEFT_NAME)

    if TEMPORAL_RESOLUTION or YEAR_RANGE or MONTH_RANGE or PFAFID_RANGE:
        sql += "WHERE "
        if TEMPORAL_RESOLUTION:
            sql += "temporal_resolution = '{}' AND ".format(TEMPORAL_RESOLUTION)
        if YEAR_RANGE:
            sql += "year >= {} AND ".format(YEAR_RANGE[0])
            sql += "year <= {} AND ".format(YEAR_RANGE[1])
        if MONTH_RANGE:
            sql += "month >= {} AND ".format(MONTH_RANGE[0])
            sql += "month <= {} AND ".format(MONTH_RANGE[1])
        if PFAFID_RANGE:
            sql += "pfafid_30spfaf06 >= {} AND ".format(PFAFID_RANGE[0])
            sql += "pfafid_30spfaf06 <= {} AND ".format(PFAFID_RANGE[1])
        # remove trailing AND
        sql = sql[:-4]

    sqls.append(sql)


    if SIMPLIFY_COLUMN_NAMES == 0:
        sqls.append(
        "CREATE TABLE {}.{} AS "
        "SELECT "
        "l.pfafid_30spfaf06, "
        "l.temporal_resolution, "
        "l.year, "
        "l.month, "
        "l.area_m2_30spfaf06, "
        "l.area_count_30spfaf06, "
        "l.ma10_pdomww_m_30spfaf06, "
        "l.ma10_pindww_m_30spfaf06, "
        "l.ma10_pirrww_m_30spfaf06, "
        "l.ma10_plivww_m_30spfaf06, "
        "l.ma10_ptotww_m_30spfaf06, "
        "l.ma10_pdomwn_m_30spfaf06, "
        "l.ma10_pindwn_m_30spfaf06, "
        "l.ma10_pirrwn_m_30spfaf06, "
        "l.ma10_plivwn_m_30spfaf06, "
        "l.ma10_ptotwn_m_30spfaf06, "
        "l.ma10_riverdischarge_m_30spfaf06, "
        "l.arid_boolean_30spfaf06, "
        "l.lowwateruse_boolean_30spfaf06, "
        "l.aridandlowwateruse_boolean_30spfaf06, "
        "l.waterstress_dimensionless_30spfaf06, "
        "r.pfaf_id, "
        "r.coast, "
        "r.geom ) "
        "FROM {}.temp_left AS l "
        "INNER JOIN {} AS r ON "
        "r.pfaf_id = l.pfafid_30spfaf06 ".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME,SIMPLIFY_TOLERANCE,OUTPUT_SCHEMA,INPUT_TABLE_RIGHT_NAME))
    elif SIMPLIFY_COLUMN_NAMES == 1:
        sqls.append(
        "CREATE TABLE {}.{} AS "
        "SELECT "
        "l.pfafid_30spfaf06 AS pfafid, "
        "l.temporal_resolution AS temp_res, "
        "l.year, "
        "l.month, "
        "l.area_m2_30spfaf06 AS area_m2, "
        "l.area_count_30spfaf06 AS area_count, "
        "l.ma10_pdomww_m_30spfaf06 AS pdomww_m, "
        "l.ma10_pindww_m_30spfaf06 AS pindww_m, "
        "l.ma10_pirrww_m_30spfaf06 AS pirrww_m, "
        "l.ma10_plivww_m_30spfaf06 AS plivww_m, "
        "l.ma10_ptotww_m_30spfaf06 AS ptotww_m, "
        "l.ma10_pdomwn_m_30spfaf06 AS pdomwn_m, "
        "l.ma10_pindwn_m_30spfaf06 AS pindwn_m, "
        "l.ma10_pirrwn_m_30spfaf06 AS pirrwn_m, "
        "l.ma10_plivwn_m_30spfaf06 AS plivwn_m, "
        "l.ma10_ptotwn_m_30spfaf06 AS ptotwn_m, "
        "l.ma10_riverdischarge_m_30spfaf06 AS q_m, "
        "l.arid_boolean_30spfaf06 AS arid, "
        "l.lowwateruse_boolean_30spfaf06 AS lowwatuse, "
        "l.aridandlowwateruse_boolean_30spfaf06 AS aridlow, "
        "l.waterstress_dimensionless_30spfaf06 AS ws_s, "
        "r.pfaf_id, "
        "r.coast, "
        "r.geom "
        "FROM {}.temp_left AS l "
        "INNER JOIN {} AS r ON "
        "r.pfaf_id = l.pfafid_30spfaf06 ".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME,OUTPUT_SCHEMA,INPUT_TABLE_RIGHT_NAME))


    sqls.append(
    "ALTER TABLE {}.{} ADD id BIGSERIAL PRIMARY KEY;".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME))
    print(sqls)

    for sql in sqls:
        print(sql)
        result = engine.execute(sql)   


# In[5]:

for key, value in DICT.items():
    NESTED_DICT = DICT[key]
    TEMPORAL_RESOLUTION = NESTED_DICT["TEMPORAL_RESOLUTION"]
    YEAR_RANGE = NESTED_DICT["YEAR_RANGE"]
    MONTH_RANGE= NESTED_DICT["MONTH_RANGE"]
    PFAFID_RANGE = NESTED_DICT["PFAFID_RANGE"]
    OUTPUT_TABLE_NAME = key
    create_sample_database()


# In[6]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:03.647104  
# 0:01:37.677627  
# 0:05:18.544848  
# 0:01:52.671324
# 3:26:07.110533
# 
# 
# 
# 
# 
# 

# In[7]:

engine.dispose()


# In[ ]:



