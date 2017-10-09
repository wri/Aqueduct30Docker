
# coding: utf-8

# # Create Line shapefile from CSV File 
# 
# * Purpose of script: Create a shapefile to visualize the flow network
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171009

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

INPUT_VERSION = 1
OUTPUT_VERSION = 4

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M10D09_RH_create_Line_Shape_File_V01/output/"

INPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)
OUTPUT_FILE_NAME = "Y2017M10D09_RH_create_Line_Shape_File_V%s.shp" %(OUTPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/Y2017M10D09_RH_create_Line_Shape_File_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D09_RH_create_Line_Shape_File_V01/output"


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[6]:

import os
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString


# In[7]:

df = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".pkl"))


# In[8]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".shp"))


# In[9]:

gdf = gdf.set_index("PFAF_ID",drop=False)


# In[10]:

print(df.shape,gdf.shape)


# In[11]:

df = df.drop_duplicates(subset="PFAF_ID",keep='first')


# In[12]:

gdf = gdf.drop_duplicates(subset="PFAF_ID",keep='first')


# In[13]:

print(df.shape,gdf.shape)


# In[14]:

gdfOut = gdf.copy()


# In[15]:

gdfOut['geometry'] = gdf.geometry.centroid


# In[16]:

df["centroid_x"] = gdfOut.geometry.x


# In[17]:

df["centroid_y"] = gdfOut.geometry.y


# In[18]:

df.head()


# In[19]:

df = df.set_index("HYBAS_ID",drop=False)   


# In[20]:

for index, row in df.iterrows():    
    df.set_value(index,"next_centroid_x",df.loc[index]["centroid_x"])
    df.set_value(index,"next_centroid_y",df.loc[index]["centroid_y"])


# In[21]:

df.head()


# In[24]:

def createLine(row):
    line = LineString([Point({row.centroid_x,centroid_y}),Point({row.to_centroid_x,to_centroid_y})])
    return line


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

d = {'Lat' : [1., 2., 3., 4.],
     'Lon' : [4., 3., 2., 1.],
     'Id': [1,1,2,2],
     'rutger':[42,43,44,45]}


# In[ ]:

df = pd.DataFrame(d)


# In[ ]:

df


# In[ ]:

geometry = [Point(xy) for xy in zip(df.Lon, df.Lat)]


# In[ ]:

point1 = Point({42,43})
point2 = Point({44,45})

lineString = LineString([point1,point2])


# In[ ]:




# In[ ]:

gdf = gpd.GeoDataFrame(df, geometry=geometry)


# In[ ]:

gdf


# In[ ]:




# In[ ]:

gdf = df.groupby(['Id'])['geometry'].apply(lambda x: LineString(x.tolist()))


# In[ ]:

# Aggregate these points with the GroupBy
geometry = df.groupby(['Id'])['geometry'].apply(lambda x: LineString(x.tolist()))


# In[ ]:

gdf = gpd.GeoDataFrame(gdf, geometry='geometry')


# In[ ]:




# In[ ]:

gdf


# In[ ]:

gdf.crs = {'init' :'epsg:4326'}


# In[ ]:

gdf.to_file(driver = 'ESRI Shapefile', filename = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME))


# In[ ]:




# In[ ]:

gdf2 = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME))


# In[ ]:

gdf2['geometry'] = gdf2.geometry.centroid


# In[ ]:

gdf2


# In[ ]:

gdf2.to_file(driver = 'ESRI Shapefile', filename = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME2))


# In[ ]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:




# In[ ]:



