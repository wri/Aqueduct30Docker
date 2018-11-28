
# coding: utf-8

# In[1]:

""" Convert hybasgadmwhymap csv files from GCS to geopackage.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_
    NAME (string) : Script name.
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

TESTING = 1
SCRIPT_NAME = "Y2018M11D28_RH_Hybasgadmwhymap_GCS_To_Gpkg_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M11D28_RH_hybasgadmwhymap_To_GCS_V01/output_V01/"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)



print("\nGCS_INPUT_PATH: ", GCS_INPUT_PATH,
      "\nec2_input_path: ", ec2_input_path, 
      "\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path
      )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('gsutil -m cp -r {GCS_INPUT_PATH}* {ec2_input_path}')


# In[6]:

files = os.listdir(ec2_input_path)


# In[7]:

files


# In[8]:

import pandas as pd
import geopandas as gpd

from shapely import wkt
from shapely.geometry import MultiPolygon, shape


# In[9]:

one_file = files[2]


# In[10]:




# In[20]:

listje = []

for one_file in files:
    input_path = "{}/{}".format(ec2_input_path,one_file)
    df = pd.read_csv(input_path,index_col=None, header=0)
    listje.append(df)


# In[22]:

df_all = pd.concat(listje, axis = 0, ignore_index = True)


# In[25]:

df_all.shape


# In[12]:

def convert_shapely_collection_to_multipolygon(fc):
    """ Removes linestrings, points etc. from shapely collections
    
    Args:
        fc (GeometryCollection): Shapely GeometryCollection object 
        
    Returns: 
        mp_out (Multipolygon): Shapely Multipolygon
    
    """
    geoms = []
    for f in fc:
        geom_type = f.geom_type
        if geom_type == "Polygon":
            geoms.append(MultiPolygon([f]))
        elif geom_type == "MultiPolygon":
            geoms.append(MultiPolygon(f))
        else:
            pass
    mp = MultiPolygon(geoms)
    return mp


# In[13]:

def wkt_to_shapely(wkt_string):
    fc = wkt.loads(wkt_string)
    try:
        output = MultiPolygon([fc])
    except:
        output = convert_shapely_collection_to_multipolygon(fc)
    return output


# In[14]:

df["geom"] = df["wkt"].apply(wkt_to_shapely)


# In[18]:

gdf = gpd.GeoDataFrame(df,geometry="geom")


# In[ ]:




# In[ ]:




# In[ ]:



