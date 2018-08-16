
# coding: utf-8

# In[1]:

"""Queries intra annual variability (SV) and stores results in csv files and stores 
in Carto
-------------------------------------------------------------------------------

"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M08D08_RH_QA_SV_Carto_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m08d02_rh_intra_annual_variability_cat_label_v01_v02"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

CARTO_OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input BQ Table : " + BQ_INPUT_TABLE_NAME +
      "\nOutput s3: " + s3_output_path + 
      "\nOutput ec2: " + ec2_output_path +
      "\nCARTO_OUTPUT_TABLE_NAME: " + CARTO_OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import sqlalchemy
import cartoframes
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
get_ipython().magic('load_ext google.cloud.bigquery')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()
creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = cartoframes.CartoContext(creds=creds)


# In[5]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

sql = """
SELECT
  pfafid_30spfaf06,
  delta_id,
  sv_riverdischarge_m_30spfaf06,
  sv_riverdischarge_score_30spfaf06,
  sv_riverdischarge_category_30spfaf06,
  sv_label_dimensionless_30spfaf06,
  sv_riverdischarge_m_delta,
  sv_riverdischarge_score_delta,
  sv_riverdischarge_category_delta,
  sv_label_dimensionless_delta,
  sv_riverdischarge_m_coalesced,
  sv_riverdischarge_score_coalesced,
  sv_riverdischarge_category_coalesced,
  sv_label_dimensionless_coalesced
FROM
  `aqueduct30.{}.{}`
ORDER BY
  pfafid_30spfaf06
""".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[7]:

print(sql)


# In[8]:

df = pd.read_gbq(query=sql,dialect="standard")


# In[9]:

df.shape


# In[10]:

output_file_name = "{}_V{:02.0f}.csv".format(SCRIPT_NAME,OUTPUT_VERSION)
print(output_file_name)
output_file_path = "{}/{}".format(ec2_output_path,output_file_name)
df.to_csv(output_file_path)


# In[11]:

# Upload result data to Carto
cc.write(df=df,
         table_name=CARTO_OUTPUT_TABLE_NAME,
         overwrite=True,
         privacy="public")


# In[12]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:19.039642

# In[ ]:



