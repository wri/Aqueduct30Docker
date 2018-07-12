
# coding: utf-8

# In[1]:

""" Add category and label for water stress. 
-------------------------------------------------------------------------------


Author: Rutger Hofste
Date: 20180712
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.  
"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D12_RH_WS_Categorization_Label_PostGIS_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d10_rh_update_waterstress_aridlowonce_postgis_v01_v02"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table: " , INPUT_TABLE_NAME, 
      "\nOutput Table: " , OUTPUT_TABLE_NAME)


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
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = "DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME)
    print(sql)
    result = engine.execute(sql)


# In[5]:

# The pythonic equivalents (not used but for QA)
def waterstress_rawvalue_to_score(r):
    # Convert raw water stress value to score; Equation from Aqueduct 2.1.
    # Results range [0,5] including start and endpoint
    if r == -1:
        score = -1
    elif r > 0:
        score = max(0,min(5,((np.log(r)-np.log(0.1))/np.log(2))+1))
    else: 
        score = -9999
    return score
  
def waterstress_score_to_category_integer(score):
    # Convert waterstress score to category. 
    # Results range [0-4]
    # Using Python's 0 based categorization.
    if score == -1:
        category = -1
    elif score >= 0 and score < 5:
        category = np.floor(score)
    elif score == 5:
        category = 4
    else:
        category = -9999
    return category
      
def waterstress_integercategory_to_labelcategory(integercategory):
    # Convert waterstress integercategory to labelcategory
    if integercategory == -1:
        label = "Arid and Low Wateruse"
    elif integercategory == 0:
        label = "Low"
    elif integercategory == 1:
        label = "Low - Medium"
    elif integercategory == 2:
        label = "Medium - High"
    elif integercategory == 3:
        label = "High"
    elif integercategory == 4:
        label = "Extremely High"
    else:
        label = "NoData"
    return label


# In[6]:

"""
df["ws_score"] = df["ws_rawvalue"].apply(waterstress_rawvalue_to_score)
df["ws_integercategory"] = df["ws_score"].apply(waterstress_score_to_category_integer)
df["ws_labelcategory"] = df["ws_integercategory"].apply(waterstress_integercategory_to_labelcategory)
"""


# In[7]:

#sql = "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)

# Water score to category integer. Note that categories are 0 (Low) based 
sql =  " CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " WITH cte AS ( "
sql += " SELECT *,"
sql +=     " CASE "
sql +=         " WHEN waterstress_score_dimensionless_30spfaf06 = -1"
sql +=             " THEN -1 "
sql +=         " WHEN waterstress_score_dimensionless_30spfaf06 < 5 AND waterstress_score_dimensionless_30spfaf06 >= 0"
sql +=             " THEN FLOOR(waterstress_score_dimensionless_30spfaf06)"
sql +=         " WHEN waterstress_score_dimensionless_30spfaf06 = 5"
sql +=             " THEN 4"
sql +=         " ELSE -9999"
sql +=     " END"
sql +=     " AS waterstress_category_dimensionless_30spfaf06"
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " )"
# Create labels
sql += " SELECT "
sql +=     " *,"
sql +=     " CASE"
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = -9999"
sql +=         " THEN 'NoData' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = -1"
sql +=         " THEN 'Arid and Low Water Use' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 0"
sql +=         " THEN 'Low' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 1"
sql +=         " THEN 'Low - Medium' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 2"
sql +=         " THEN 'Medium - High' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 3"
sql +=         " THEN 'High' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 4"
sql +=         " THEN 'Extremely High' "
sql +=     " ELSE 'error, check score'"
sql +=     " END AS waterstress_label_dimensionless_30spfaf06"
sql += " FROM cte;"


# In[8]:

result = engine.execute(sql)


# In[9]:

sql_index = "CREATE INDEX {}pfafid_30spfaf06 ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"pfafid_30spfaf06")


# In[10]:

result = engine.execute(sql_index)


# In[11]:

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[12]:

result = engine.execute(sql_index2)


# In[13]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[14]:

result = engine.execute(sql_index3)


# In[15]:

engine.dispose()


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:25:34.359384 

# In[ ]:



