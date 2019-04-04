
# coding: utf-8

# In[5]:

""" Create table for upstream and downstream relation.
-------------------------------------------------------------------------------
Create a csv file with all PFAFID upstream and downstream.

Author: Rutger Hofste
Date: 20190228
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:


"""

SCRIPT_NAME = "Y2019M02D28_RH_Hydrobasin_Upstream_Downstream_Normalized_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "hybas06_v04"



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[16]:

import sqlalchemy
import pandas as pd
import geopandas as gpd


# In[ ]:

pd.read_sql(sql=con=)


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[17]:

sql = "select hybas_id, next_down, next_sink, main_bas, endo, coast, pfaf_id from {}".format(INPUT_TABLE_NAME)


# In[19]:

# load geodataframe from postGIS
df =pd.read_sql(sql=sql,con=engine)


# In[20]:

df.head()


# In[24]:

df.tail()


# In[26]:

def find_downstream(hybas_id):
    """ Create a dataframe with all downstream basins
    
    
    """
    test = df.loc[df["hybas_id"==hybas_id]]
    return test
    


# In[29]:

df.loc[df["hybas_id"] == 9060121950]


# In[ ]:



