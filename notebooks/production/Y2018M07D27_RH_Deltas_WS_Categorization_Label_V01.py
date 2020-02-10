
# coding: utf-8

# In[1]:

""" Add category and label for water stress for deltas. 
-------------------------------------------------------------------------------
Y2020M02D06 Update output 3-4 input 2-3

Author: Rutger Hofste
Date: 20180727
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
SCRIPT_NAME = 'Y2018M07D27_RH_Deltas_WS_Categorization_Label_V01'
OUTPUT_VERSION = 4

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d27_rh_deltas_update_waterstress_aridlowonce_v01_v03"
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
        label = "Low (<10%)"
    elif integercategory == 1:
        label = "Low - Medium (10-20%)"
    elif integercategory == 2:
        label = "Medium - High (20-40%)"
    elif integercategory == 3:
        label = "High (40-80%)"
    elif integercategory == 4:
        label = "Extremely High (>80%)"
    else:
        label = "NoData"
    return label


# In[6]:

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
sql +=     " AS waterstress_category_dimensionless_30spfaf06,"

sql +=     " CASE "
sql +=         " WHEN waterdepletion_score_dimensionless_30spfaf06 = -1"
sql +=             " THEN -1 "
sql +=         " WHEN waterdepletion_score_dimensionless_30spfaf06 < 5 AND waterdepletion_score_dimensionless_30spfaf06 >= 0"
sql +=             " THEN FLOOR(waterdepletion_score_dimensionless_30spfaf06)"
sql +=         " WHEN waterdepletion_score_dimensionless_30spfaf06 = 5"
sql +=             " THEN 4"
sql +=         " ELSE -9999"
sql +=     " END"
sql +=     " AS waterdepletion_category_dimensionless_30spfaf06"

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
sql +=         " THEN 'Low (<10%)' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 1"
sql +=         " THEN 'Low - Medium (10-20%)' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 2"
sql +=         " THEN 'Medium - High (20-40%)' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 3"
sql +=         " THEN 'High (40-80%)' "
sql +=     " WHEN waterstress_category_dimensionless_30spfaf06 = 4"
sql +=         " THEN 'Extremely High (>80%)' "
sql +=     " ELSE 'error, check score'"
sql +=     " END AS waterstress_label_dimensionless_30spfaf06,"

sql +=     " CASE"
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = -9999"
sql +=         " THEN 'NoData' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = -1"
sql +=         " THEN 'Arid and Low Water Use' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = 0"
sql +=         " THEN 'Low (<5%)' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = 1"
sql +=         " THEN 'Low - Medium (5-25%)' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = 2"
sql +=         " THEN 'Medium - High (25-50%)' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = 3"
sql +=         " THEN 'High (50-75%)' "
sql +=     " WHEN waterdepletion_category_dimensionless_30spfaf06 = 4"
sql +=         " THEN 'Extremely High (>75%)' "
sql +=     " ELSE 'error, check score'"
sql +=     " END AS waterdepletion_label_dimensionless_30spfaf06"

sql += " FROM cte;"


# In[7]:

result = engine.execute(text(sql))


# In[8]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[9]:

result = engine.execute(sql_index)


# In[10]:

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[11]:

result = engine.execute(sql_index2)


# In[12]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[13]:

result = engine.execute(sql_index3)


# In[14]:

engine.dispose()


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:02.116257  
# 0:00:09.629002

# In[ ]:



