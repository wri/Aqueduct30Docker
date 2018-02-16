
# coding: utf-8

# ### Y2018M02D15_RH_GDBD_Merge_V01
# 
# * Purpose of script: This script will reproject GDBD basins and streams and merge them. 
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20180215
# 
# 
# Basins and Streams extracted from GDBD databases downloaded from:
# 
# http://www.cger.nies.go.jp/db/gdbd/data/Africa.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Asia.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Europe.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Oceania.zip
# http://www.cger.nies.go.jp/db/gdbd/data/N_America.zip
# http://www.cger.nies.go.jp/db/gdbd/data/S_America.zip
# 
# Unzipped, streams and basins opened with ArcGIS Desktop 10.5 and exported to shapefiles. 
# 
# Shapefile of Asia caused problems with reprojection. Use Reproject in ArcMap. 
# 
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2018M02D15_RH_GDBD_Merge_V01"

EC2_INPUT_PATH  = ("/volumes/data/{}/input/").format(SCRIPT_NAME)
EC2_OUTPUT_PATH = ("/volumes/data/{}/output/").format(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M02D15_RH_GDBD_Basins_Streams_SHP_V01"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/{}/output".format(SCRIPT_NAME)


VERSION = 6


# In[3]:

print(EC2_OUTPUT_PATH)


# In[4]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[6]:

import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd
import getpass
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

get_ipython().magic('matplotlib inline')


# In[7]:

target_crs = {'init': 'epsg:4326'}


# In[8]:

shapes = ["basins","streams"]
continents = ["af","as","eu","na","oc","sa"]

df = pd.DataFrame()

for shape in shapes:
    for continent in continents:
        dictje = {}
        dictje["path"] = "{}{}/{}_{}.shp".format(EC2_INPUT_PATH,shape,continent,shape)
        print(dictje["path"])
        dictje["shape"] = shape
        dictje["continent"] = continent
        df = df.append(pd.Series(dictje),ignore_index=True)
    
        


# In[9]:

result_dict = {}

for shape in shapes:
    df_selection = df.loc[df['shape'] == shape]
    gdf_out = gpd.GeoDataFrame()
    for index, row in df_selection.iterrows():
        gdf = gpd.read_file(row["path"])
        gdf2 = gdf.to_crs(target_crs)
        gdf_out = gdf_out.append(gdf2)
                          
    
    output_path_shp = "{}GDBD_{}_EPSG4326_V{:02.0f}.shp".format(EC2_OUTPUT_PATH,shape,VERSION)
    output_path_pkl = "{}GDBD_{}_EPSG4326_V{:02.0f}.pkl".format(EC2_OUTPUT_PATH,shape,VERSION)
    print(output_path_shp)
    gdf_out.to_file(output_path_shp,driver='ESRI Shapefile')
    gdf_out.to_pickle(output_path_pkl)
    result_dict[shape] = gdf_out
    


# In[10]:

get_ipython().system('aws s3 cp --recursive {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH}')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



