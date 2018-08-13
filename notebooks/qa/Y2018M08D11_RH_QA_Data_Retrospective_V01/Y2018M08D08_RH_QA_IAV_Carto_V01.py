
# coding: utf-8

# In[1]:

"""Queries inter annual variability and stores results in csv files and stores 
in Carto
-------------------------------------------------------------------------------

"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M08D08_RH_QA_IAV_Carto_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m07d31_rh_inter_av_cat_label_v01_v01"

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


# In[5]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()
creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = cartoframes.CartoContext(creds=creds)


# In[6]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[7]:

sql = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  month,
  year,
  iav_riverdischarge_m_30spfaf06,
  iav_riverdischarge_score_30spfaf06,
  iav_riverdischarge_category_30spfaf06,
  iav_riverdischarge_label_30spfaf06
FROM
  `aqueduct30.{}.{}`
WHERE
  year = 2014
ORDER BY
  pfafid_30spfaf06,
  temporal_resolution,
  year,
  month
""".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[8]:

print(sql)


# In[9]:

df = pd.read_gbq(query=sql,dialect="standard")


# In[10]:

df.shape


# In[11]:

temporal_resolutions = ["month","year"]


# In[12]:

for temporal_resolution in temporal_resolutions:
    if temporal_resolution == "year":
        month = 12
        df_selected = df.loc[(df["month"]==month) & (df["temporal_resolution"]==temporal_resolution)]
        df_selected = df_selected.sort_index(axis=1)
        output_file_name = "{}_V{:02.0f}_{}_Y2014M{:02.0f}.csv".format(SCRIPT_NAME,OUTPUT_VERSION,temporal_resolution,month)
        print(output_file_name)
        output_file_path = "{}/{}".format(ec2_output_path,output_file_name)
        df_selected .to_csv(output_file_path)
        
    elif temporal_resolution == "month":
        for month in range(1,13):
            df_selected = df.loc[(df["month"]==month) & (df["temporal_resolution"]==temporal_resolution)]
            df_selected = df_selected.sort_index(axis=1)
            output_file_name = "{}_V{:02.0f}_{}_Y2014M{:02.0f}.csv".format(SCRIPT_NAME,OUTPUT_VERSION,temporal_resolution,month)
            print(output_file_name)
            output_file_path = "{}/{}".format(ec2_output_path,output_file_name)
            df_selected .to_csv(output_file_path)


# In[13]:

# Upload result data to Carto
cc.write(df=df,
         table_name=CARTO_OUTPUT_TABLE_NAME,
         overwrite=True,
         privacy="public")


# In[14]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 

# In[ ]:



