
# coding: utf-8

# In[2]:

""" Convert riverdischarge data to flux and simplify table.
-------------------------------------------------------------------------------


SCRIP NOT USED. converting before uploading instead. 


Demand data is stored in PostGIS as flux data. Riverdischarge is calculated in
volumes due to the several sources of the data (mainchannel + sinks).  

Author: Rutger Hofste
Date: 20180522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 0
OVERWRITE = 1 
SCRIPT_NAME = "Y2018M05D22_RH_Post_Process_Riverdischarge_PostGIS_30sPfaf06_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

S3_INPUT_PATH_AREA = "s3://wri-projects/Aqueduct30/processData/Y2018M04D20_RH_Zonal_Stats_Area_EE_V01/output_V02"
S3_INPUT_PATH_RIVERDISCHARGE = "s3://wri-projects/Aqueduct30/processData/Y2018M04D20_RH_Zonal_Stats_Area_EE_V01/output_V02"


ec2_input_path_area = "/volumes/data/{}/input_area_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_input_path_riverdischarge = "/volumes/data/{}/input_area_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 area: " + S3_INPUT_PATH_AREA)


# In[ ]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()


# In[ ]:

import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[ ]:

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()


# In[ ]:

query = "SELECT * FROM area_m2_5minpfaf06 LIMIT 100"


# In[ ]:

df_test = pd.read_sql_query(query, connection)


# In[ ]:

df_test.set_index("pfafid_5minpfaf06",inplace=True)


# In[ ]:

df_test.to_sql("test02",engine,if_exists="fail",index=False)


# In[ ]:



